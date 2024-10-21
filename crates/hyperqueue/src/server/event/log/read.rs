use crate::common::serialization::bincode_config;
use crate::server::event::log::HQ_JOURNAL_HEADER;
use crate::server::event::Event;
use crate::HQ_VERSION;
use anyhow::{anyhow, bail};
use bincode::Options;
use std::fs::File;
use std::io::Read;
use std::io::{BufReader, Seek};
use std::ops::Deref;
use std::path::Path;

/// Reads events from a file in a streaming fashion.
/// EventLogReader is able load a file that was not fully written; in this case `partial_data_error` is set to `true`.
/// `position` points to the end of correct data; therefore, if the file is truncated to the
/// `position` length it will contains only valid events and the incomplete event is discarded.
pub struct JournalReader {
    source: BufReader<File>,
    position: u64,
    size: u64,
    partial_data_error: bool,
}

impl JournalReader {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let raw_file = File::open(path)?;
        let size = raw_file.metadata()?.len();
        let mut file = BufReader::new(raw_file);
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
        Ok(Self {
            position: file.stream_position()?,
            size,
            source: file,
            partial_data_error: false,
        })
    }

    pub fn contains_partial_data(&self) -> bool {
        self.partial_data_error
    }

    pub fn position(&self) -> u64 {
        self.position
    }
}

impl Iterator for &mut JournalReader {
    type Item = Result<Event, bincode::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.position = self.source.stream_position().unwrap();
        if self.position == self.size {
            return None;
        }
        match bincode_config().deserialize_from(&mut self.source) {
            Ok(event) => Some(Ok(event)),
            Err(error) => match error.deref() {
                bincode::ErrorKind::Io(e)
                    if matches!(e.kind(), std::io::ErrorKind::UnexpectedEof) =>
                {
                    self.partial_data_error = true;
                    None
                }
                _ => Some(Err(error)),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::server::event::log::{JournalReader, JournalWriter};
    use crate::server::event::payload::EventPayload;
    use crate::server::event::Event;
    use chrono::Utc;
    use std::fs::{File, OpenOptions};
    use std::io::Write;
    use tako::gateway::LostWorkerReason;
    use tempfile::TempDir;

    #[test]
    fn read_empty_file() {
        let tmpdir = TempDir::with_prefix("hq").unwrap();
        let path = tmpdir.path().join("foo");
        File::create(&path).unwrap();

        assert!(JournalReader::open(&path).is_err());
    }

    #[test]
    fn read_invalid_header_version() {
        let tmpdir = TempDir::with_prefix("hq").unwrap();
        let path = tmpdir.path().join("foo");
        {
            let mut file = File::create(&path).unwrap();
            file.write_all("hqjlxxxx".as_bytes()).unwrap();
            file.flush().unwrap();
        }
        assert!(JournalReader::open(&path).is_err());
    }

    #[test]
    fn read_no_events() {
        let tmpdir = TempDir::with_prefix("hq").unwrap();
        let path = tmpdir.path().join("foo");
        {
            let writer = JournalWriter::create_or_append(&path, None).unwrap();
            writer.finish().unwrap();
        }

        let mut reader = JournalReader::open(&path).unwrap();
        assert!((&mut reader).next().is_none());
    }

    #[test]
    fn test_not_fully_written_journal() {
        let tmpdir = TempDir::with_prefix("hq").unwrap();
        let path = tmpdir.path().join("foo");

        {
            let mut writer = JournalWriter::create_or_append(&path, None).unwrap();
            for _id in 0..100 {
                writer
                    .store(Event {
                        time: Utc::now(),
                        payload: EventPayload::WorkerLost(
                            0.into(),
                            LostWorkerReason::ConnectionLost,
                        ),
                    })
                    .unwrap();
            }
        }

        let size;
        {
            let f = OpenOptions::new().write(true).open(&path).unwrap();
            size = f.metadata().unwrap().len();
            assert!(size > 500);
            f.set_len(size - 1).unwrap(); // Truncate file
        }

        let truncate;
        {
            let mut reader = JournalReader::open(&path).unwrap();
            let mut count = 0;
            for item in &mut reader {
                item.unwrap();
                count += 1;
            }
            assert!(reader.contains_partial_data());
            assert_eq!(count, 99);
            assert!(reader.position() < size - 1);
            truncate = reader.position();
        }

        {
            let f = OpenOptions::new().write(true).open(&path).unwrap();
            f.set_len(truncate).unwrap(); // Truncate file
        }

        {
            let mut reader = JournalReader::open(&path).unwrap();
            let mut count = 0;
            for item in &mut reader {
                item.unwrap();
                count += 1;
            }
            assert!(!reader.contains_partial_data());
            assert_eq!(count, 99);
        }
    }

    #[test]
    fn roundtrip_exhaust_buffer() {
        let tmpdir = TempDir::with_prefix("hq").unwrap();
        let path = tmpdir.path().join("foo");

        {
            let mut writer = JournalWriter::create_or_append(&path, None).unwrap();
            for _id in 0..100000 {
                writer
                    .store(Event {
                        time: Utc::now(),
                        payload: EventPayload::WorkerLost(
                            0.into(),
                            LostWorkerReason::ConnectionLost,
                        ),
                    })
                    .unwrap();
            }
            writer.finish().unwrap();
        }

        let mut reader = JournalReader::open(&path).unwrap();
        for _id in 0..100000 {
            let event = (&mut reader).next().unwrap().unwrap();
            assert!(matches!(
                event.payload,
                EventPayload::WorkerLost(id, LostWorkerReason::ConnectionLost)
                if id.as_num() == 0
            ));
        }
        assert!((&mut reader).next().is_none());
    }

    #[test]
    fn streaming_read_partial() {
        let tmpdir = TempDir::with_prefix("hq").unwrap();
        let path = tmpdir.path().join("foo");
        let mut writer = JournalWriter::create_or_append(&path, None).unwrap();

        let time = Utc::now();
        writer
            .store(Event {
                time,
                payload: EventPayload::AllocationFinished(0, "a".to_string()),
            })
            .unwrap();
        writer.flush().unwrap();

        let mut reader = JournalReader::open(&path).unwrap();
        let event = (&mut reader).next().unwrap().unwrap();
        assert_eq!(event.time.timestamp_millis(), time.timestamp_millis());
        assert!(matches!(
            event.payload,
            EventPayload::AllocationFinished(0, _)
        ));
    }
}
