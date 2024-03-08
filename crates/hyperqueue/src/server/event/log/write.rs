use crate::server::event::log::canonical_header;
use crate::server::event::{bincode_config, Event};
use bincode::Options;
use std::path::Path;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

/// Streams monitoring events into a file on disk.
pub struct EventLogWriter {
    file: File,
    buffer: Vec<u8>,
}

const BUF_MAX_SIZE: usize = 16 * 1024;

impl EventLogWriter {
    pub async fn create_or_append(path: &Path) -> anyhow::Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        let header = bincode_config().serialize(&canonical_header())?;
        file.write_all(&header).await?;
        file.flush().await?;

        // Keep buffer capacity larger than max size to avoid reallocation if we overflow
        // the buffer.
        let buffer = Vec::with_capacity(BUF_MAX_SIZE * 2);
        Ok(Self { file, buffer })
    }

    #[inline]
    pub async fn store(&mut self, event: Event) -> anyhow::Result<()> {
        bincode_config().serialize_into(&mut self.buffer, &event)?;
        if self.is_buffer_full() {
            self.write_buffer().await?;
        }
        Ok(())
    }

    pub async fn flush(&mut self) -> anyhow::Result<()> {
        if !self.buffer.is_empty() {
            self.write_buffer().await?;
        }
        self.file.flush().await?;
        Ok(())
    }

    pub async fn finish(mut self) -> anyhow::Result<()> {
        self.flush().await?;
        self.file.shutdown().await?;
        Ok(())
    }

    async fn write_buffer(&mut self) -> tokio::io::Result<()> {
        self.file.write_all(&self.buffer).await?;
        self.buffer.clear();
        Ok(())
    }

    #[inline]
    fn is_buffer_full(&self) -> bool {
        self.buffer.len() >= BUF_MAX_SIZE
    }
}
