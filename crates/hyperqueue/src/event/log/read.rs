use crate::event::log::{canonical_header, LogFileHeader};
use crate::event::MonitoringEvent;
use anyhow::anyhow;
use flate2::read::GzDecoder;
use rmp_serde::decode::Error;
use std::fs::File;
use std::io::ErrorKind;
use std::path::Path;

/// Reads events from a file in a streaming fashion.
pub struct EventLogReader {
    source: GzDecoder<File>,
}

impl EventLogReader {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let mut file = File::open(path)?;
        let header: LogFileHeader = rmp_serde::from_read(&mut file)
            .map_err(|error| anyhow!("Cannot load HQ event log file header: {error:?}"))?;

        let expected_header = canonical_header();
        if header != expected_header {
            return Err(anyhow!(
                "Invalid HQ event log file header.\nFound: {header:?}\nExpected: {expected_header:?}"
            ));
        }

        let source = GzDecoder::new(file);
        Ok(Self { source })
    }
}

impl Iterator for EventLogReader {
    type Item = Result<MonitoringEvent, rmp_serde::decode::Error>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match rmp_serde::from_read(&mut self.source) {
            Ok(event) => Some(Ok(event)),
            Err(Error::InvalidMarkerRead(error))
                if matches!(error.kind(), ErrorKind::UnexpectedEof) =>
            {
                None
            }
            Err(error) => Some(Err(error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::event::events::MonitoringEventPayload;
    use crate::event::log::{
        EventLogReader, EventLogWriter, LogFileHeader, HQ_LOG_HEADER, HQ_LOG_VERSION,
    };
    use crate::event::MonitoringEvent;
    use std::fs::File;
    use std::io::Write;
    use std::time::SystemTime;
    use tako::messages::gateway::LostWorkerReason;
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
                    header: HQ_LOG_HEADER.into(),
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
            let writer = EventLogWriter::create(&path).await.unwrap();
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
            let mut writer = EventLogWriter::create(&path).await.unwrap();
            for id in 0..100000 {
                writer
                    .store(MonitoringEvent {
                        id,
                        time: SystemTime::now(),
                        payload: MonitoringEventPayload::WorkerLost(
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
                MonitoringEventPayload::WorkerLost(id, LostWorkerReason::ConnectionLost)
                if id.as_num() == 0
            ));
        }
        assert!(reader.next().is_none());
    }
}
