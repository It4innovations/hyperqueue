use crate::server::event::log::{canonical_header, EventChunkHeader, LogFileHeader};
use crate::server::event::MonitoringEvent;
use anyhow::anyhow;
use flate2::read::GzDecoder;
use rmp_serde::decode::Error;
use std::fs::File;
use std::io::{Cursor, ErrorKind, Seek, SeekFrom};
use std::os::unix::fs::FileExt;
use std::path::Path;

/// Reads events from a file in a streaming fashion.
pub struct EventLogReader {
    source: File,
    current_chunk: GzDecoder<Cursor<Vec<u8>>>,
}

impl EventLogReader {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let mut source = File::open(path)?;
        let header: LogFileHeader = rmp_serde::from_read(&mut source)
            .map_err(|error| anyhow!("Cannot load HQ event log file header: {error:?}"))?;

        let expected_header = canonical_header();
        if header != expected_header {
            return Err(anyhow!(
                "Invalid HQ event log file header.\nFound: {header:?}\nExpected: {expected_header:?}"
            ));
        }
        Ok(Self {
            source,
            current_chunk: GzDecoder::new(Default::default()),
        })
    }
}

impl Iterator for EventLogReader {
    type Item = Result<MonitoringEvent, rmp_serde::decode::Error>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let event: Result<MonitoringEvent, Error> = rmp_serde::from_read(&mut self.current_chunk);
        match event {
            Ok(event) => Some(Ok(event)),
            Err(Error::InvalidMarkerRead(error))
                if matches!(error.kind(), ErrorKind::UnexpectedEof) =>
            {
                let next_header: Result<EventChunkHeader, Error> =
                    rmp_serde::from_read(&mut self.source);
                match next_header {
                    Ok(chunk_header) => {
                        let begin_index = self.source.seek(SeekFrom::Current(0_i64)).ok()?;
                        let mut buffer = vec![0; chunk_header.chunk_length];
                        self.source.read_at(&mut buffer, begin_index as u64).ok()?;
                        self.current_chunk = GzDecoder::new(Cursor::new(buffer));
                        let _ = self
                            .source
                            .seek(SeekFrom::Current(chunk_header.chunk_length as i64))
                            .ok()?;
                        self.next()
                    }
                    Err(Error::InvalidMarkerRead(error))
                        if matches!(error.kind(), ErrorKind::UnexpectedEof) =>
                    {
                        None
                    }
                    Err(error) => Some(Err(error)),
                }
            }
            Err(error) => Some(Err(error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::server::event::events::MonitoringEventPayload;
    use crate::server::event::log::{
        EventLogReader, EventLogWriter, LogFileHeader, HQ_LOG_HEADER, HQ_LOG_VERSION,
    };
    use crate::server::event::MonitoringEvent;
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

    #[tokio::test]
    async fn streaming_read_partial() {
        let tmpdir = TempDir::new("hq").unwrap();
        let path = tmpdir.path().join("foo");
        let mut writer = EventLogWriter::create(&path).await.unwrap();

        let time = SystemTime::now();
        writer
            .store(MonitoringEvent {
                id: 42,
                time,
                payload: MonitoringEventPayload::AllocationFinished(0, "a".to_string()),
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
            MonitoringEventPayload::AllocationFinished(0, _)
        ));
    }
}
