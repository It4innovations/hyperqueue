use crate::server::event::log::HQ_JOURNAL_HEADER;
use crate::server::event::{bincode_config, Event};
use crate::HQ_VERSION;
use anyhow::{anyhow, bail};
use bincode::Options;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::ops::Deref;
use std::path::Path;

/// Reads events from a file in a streaming fashion.
pub struct EventLogReader {
    source: BufReader<File>,
}

impl EventLogReader {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let mut file = BufReader::new(File::open(path)?);
        let mut header = [0u8; 8];
        file.read_exact(&mut header)?;
        if header != HQ_JOURNAL_HEADER {
            bail!("Invalid journal format");
        }
        let hq_version: String = bincode_config()
            .deserialize_from(&mut file)
            .map_err(|error| anyhow!("Cannot load HQ event log file header: {error:?}"))?;
        if hq_version != HQ_VERSION {
            bail!("Version of journal {hq_version} does not match with {HQ_VERSION}");
        }
        Ok(Self { source: file })
    }
}

impl Iterator for EventLogReader {
    type Item = Result<Event, bincode::Error>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match bincode_config().deserialize_from(&mut self.source) {
            Ok(event) => Some(Ok(event)),
            Err(error) => match error.deref() {
                bincode::ErrorKind::Io(e)
                    if matches!(e.kind(), std::io::ErrorKind::UnexpectedEof) =>
                {
                    None
                }
                _ => Some(Err(error)),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::server::event::log::{
        EventLogReader, EventLogWriter, LogFileHeader, HQ_JOURNAL_HEADER, HQ_LOG_VERSION,
    };
    use crate::server::event::payload::EventPayload;
    use crate::server::event::Event;
    use std::fs::File;
    use std::io::Write;
    use std::time::SystemTime;
    use tako::gateway::LostWorkerReason;
    use tempdir::TempDir;

    #[test]
    fn read_empty_file() {
        let tmpdir = TempDir::new("hq").unwrap();
        let path = tmpdir.path().join("foo");
        File::create(&path).unwrap();

        assert!(EventLogReader::open(&path).is_err());
    }

    #[test]
    fn read_invalid_header_version() {
        let tmpdir = TempDir::new("hq").unwrap();
        let path = tmpdir.path().join("foo");
        {
            let mut file = File::create(&path).unwrap();
            rmp_serde::encode::write(
                &mut file,
                &LogFileHeader {
                    header: HQ_JOURNAL_HEADER.into(),
                    version: HQ_LOG_VERSION + 1,
                },
            )
            .unwrap();
            file.flush().unwrap();
        }

        assert!(EventLogReader::open(&path).is_err());
    }

    #[tokio::test]
    async fn read_no_events() {
        let tmpdir = TempDir::new("hq").unwrap();
        let path = tmpdir.path().join("foo");
        {
            let writer = EventLogWriter::create_or_append(&path).await.unwrap();
            writer.finish().await.unwrap();
        }

        let mut reader = EventLogReader::open(&path).unwrap();
        assert!(reader.next().is_none());
    }

    #[tokio::test]
    async fn roundtrip_exhaust_buffer() {
        let tmpdir = TempDir::new("hq").unwrap();
        let path = tmpdir.path().join("foo");

        {
            let mut writer = EventLogWriter::create_or_append(&path).await.unwrap();
            for id in 0..100000 {
                writer
                    .store(Event {
                        id,
                        time: SystemTime::now(),
                        payload: EventPayload::WorkerLost(
                            0.into(),
                            LostWorkerReason::ConnectionLost,
                        ),
                    })
                    .await
                    .unwrap();
            }
            writer.finish().await.unwrap();
        }

        let mut reader = EventLogReader::open(&path).unwrap();
        for id in 0..100000 {
            let event = reader.next().unwrap().unwrap();
            assert_eq!(event.id, id);
            assert!(matches!(
                event.payload,
                EventPayload::WorkerLost(id, LostWorkerReason::ConnectionLost)
                if id.as_num() == 0
            ));
        }
        assert!(reader.next().is_none());
    }

    #[tokio::test]
    async fn streaming_read_partial() {
        let tmpdir = TempDir::new("hq").unwrap();
        let path = tmpdir.path().join("foo");
        let mut writer = EventLogWriter::create_or_append(&path).await.unwrap();

        let time = SystemTime::now();
        writer
            .store(Event {
                id: 42,
                time,
                payload: EventPayload::AllocationFinished(0, "a".to_string()),
            })
            .await
            .unwrap();
        writer.flush().await.unwrap();

        let mut reader = EventLogReader::open(&path).unwrap();
        let event = reader.next().unwrap().unwrap();
        assert_eq!(event.id, 42);
        assert_eq!(event.time, time);
        assert!(matches!(
            event.payload,
            EventPayload::AllocationFinished(0, _)
        ));
    }
}
