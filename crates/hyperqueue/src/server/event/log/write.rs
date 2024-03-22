use crate::server::event::log::HQ_JOURNAL_HEADER;
use crate::server::event::{bincode_config, Event};
use crate::HQ_VERSION;
use bincode::Options;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, Write};
use std::path::Path;

/// Streams monitoring events into a file on disk.
pub struct EventLogWriter {
    file: BufWriter<File>,
}

impl EventLogWriter {
    pub fn create_or_append(path: &Path) -> anyhow::Result<Self> {
        let mut file = if !path.exists() {
            let mut file = BufWriter::new(File::create(path)?);
            if file.stream_position()? == 0 {
                file.write_all(HQ_JOURNAL_HEADER)?;
                bincode_config().serialize_into(&mut file, HQ_VERSION)?;
                file.flush()?;
            }
            file
        } else {
            BufWriter::new(OpenOptions::new().create(true).append(true).open(path)?)
        };
        Ok(Self { file })
    }

    pub fn store(&mut self, event: Event) -> anyhow::Result<()> {
        bincode_config().serialize_into(&mut self.file, &event)?;
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
