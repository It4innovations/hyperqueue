use crate::HQ_VERSION;
use crate::common::serialization::SerializationConfig;
use crate::server::event::journal::HQ_JOURNAL_HEADER;
use crate::server::event::{Event, EventSerializationConfig};
use bincode::Options;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::Path;

/// Streams monitoring events into a file on disk.
pub struct JournalWriter {
    file: BufWriter<File>,
}

impl JournalWriter {
    pub fn create_or_append(path: &Path, truncate: Option<u64>) -> anyhow::Result<Self> {
        let mut raw_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        let position = if let Some(size) = truncate {
            raw_file.set_len(size)?;
            size
        } else {
            raw_file.metadata()?.len()
        };

        raw_file.seek(SeekFrom::Start(position))?;
        let mut file = BufWriter::new(raw_file);

        if position == 0 && file.stream_position()? == 0 {
            Self::write_header(&mut file)?;
        };

        Ok(Self { file })
    }

    pub fn create(path: &Path) -> anyhow::Result<Self> {
        let raw_file = File::create(path)?;
        let mut file = BufWriter::new(raw_file);
        Self::write_header(&mut file)?;
        Ok(Self { file })
    }

    fn write_header(mut file: &mut BufWriter<File>) -> anyhow::Result<()> {
        file.write_all(HQ_JOURNAL_HEADER)?;
        EventSerializationConfig::config().serialize_into(&mut file, HQ_VERSION)?;
        file.flush()?;
        Ok(())
    }

    pub fn store(&mut self, event: Event) -> anyhow::Result<()> {
        EventSerializationConfig::config().serialize_into(&mut self.file, &event)?;
        Ok(())
    }

    pub fn flush(&mut self) -> anyhow::Result<()> {
        self.file.flush()?;
        Ok(())
    }

    pub fn finish(mut self) -> anyhow::Result<()> {
        self.file.flush()?;
        Ok(())
    }
}
