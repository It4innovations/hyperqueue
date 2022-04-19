use crate::server::event::log::write::EventChunkHeader;
use crate::server::event::log::{canonical_header, LogFileHeader};
use crate::server::event::{MonitoringEvent, MonitoringEventId};
use anyhow::anyhow;
use flate2::read::GzDecoder;
use rmp_serde::decode::Error;
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::os::unix::fs::FileExt;
use std::path::Path;

/// Reads events from a file in a streaming fashion.
pub struct EventLogReader {
    source: File,

    event_chunks: Vec<(MonitoringEventId, EventChunk)>,
    current_chunk_index: usize,

    current_decoded_chunk: Vec<MonitoringEvent>,
    index_in_decoded_chunk: usize,
}

struct EventChunk {
    begin_index: usize,
    size: usize,
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
        let event_chunks = build_index(&mut source)?;
        Ok(Self {
            source,
            event_chunks,
            current_chunk_index: 0,
            current_decoded_chunk: vec![],
            index_in_decoded_chunk: 0,
        })
    }

    /// Decodes the next event chunk, returns false if no next chunk exists.
    fn decode_next_chunk(&mut self) -> anyhow::Result<bool> {
        self.current_decoded_chunk.clear();
        self.index_in_decoded_chunk = 0;

        if self.current_chunk_index >= self.event_chunks.len() {
            return Ok(false);
        }

        let chunk = &self.event_chunks[self.current_chunk_index];
        let mut buffer = vec![0; chunk.1.size];
        self.source
            .read_at(&mut buffer, chunk.1.begin_index as u64)?;

        let mut decoder = GzDecoder::new(&buffer[..]);
        loop {
            let event: Result<MonitoringEvent, Error> = rmp_serde::from_read(&mut decoder);
            match event {
                Ok(event) => {
                    self.current_decoded_chunk.push(event);
                }
                Err(Error::InvalidMarkerRead(_)) => {
                    // finished reading current chunk
                    self.current_chunk_index += 1;
                    return Ok(true);
                }
                Err(error) => {
                    return Ok(false);
                }
            }
        }
    }
}

/// Read all chunk headers from the file and store them.
fn build_index(mut events_file: &mut File) -> anyhow::Result<Vec<(MonitoringEventId, EventChunk)>> {
    let mut chunks: Vec<(MonitoringEventId, EventChunk)> = vec![];
    loop {
        let header: Result<EventChunkHeader, Error> = rmp_serde::from_read(&mut events_file);
        match header {
            Ok(chunk) => {
                let current = events_file.seek(SeekFrom::Current(0_i64))?;
                chunks.push((
                    chunk.first_event_id,
                    EventChunk {
                        begin_index: current as usize,
                        size: chunk.chunk_length,
                    },
                ));
                let _ = events_file.seek(SeekFrom::Current(chunk.chunk_length as i64))?;
            }
            //todo: if invalid marker read, ok, otherwise propagate error
            Err(_error) => {
                events_file.seek(SeekFrom::Start(0))?;
                return Ok(chunks);
            }
        }
    }
}

impl Iterator for EventLogReader {
    type Item = Result<MonitoringEvent, rmp_serde::decode::Error>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index_in_decoded_chunk < self.current_decoded_chunk.len() {
            let next_event = self.current_decoded_chunk[self.index_in_decoded_chunk].clone();
            self.index_in_decoded_chunk += 1;
            Some(Ok(next_event))
        } else {
            match self.decode_next_chunk() {
                Ok(has_more) if has_more => {
                    let next_event =
                        self.current_decoded_chunk[self.index_in_decoded_chunk].clone();
                    self.index_in_decoded_chunk += 1;
                    Some(Ok(next_event))
                }
                _ => None,
            }
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
