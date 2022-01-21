use crate::event::log::canonical_header;
use crate::event::MonitoringEvent;
use async_compression::tokio::write::GzipEncoder;
use async_compression::Level;
use std::path::Path;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

/// Streams monitoring events into a file on disk.
pub struct EventLogWriter {
    file: GzipEncoder<File>,
    buffer: Vec<u8>,
}

const BUF_MAX_SIZE: usize = 16 * 1024;

impl EventLogWriter {
    pub async fn create(path: &Path) -> anyhow::Result<Self> {
        let mut file = File::create(path).await?;
        let header = rmp_serde::encode::to_vec(&canonical_header())?;
        file.write_all(&header).await?;
        file.flush().await?;

        let file = GzipEncoder::with_quality(file, Level::Fastest);

        // Keep buffer capacity larger than max size to avoid reallocation if we overflow
        // the buffer.
        let buffer = Vec::with_capacity(BUF_MAX_SIZE * 2);
        Ok(Self { file, buffer })
    }

    #[inline]
    pub async fn store(&mut self, event: MonitoringEvent) -> anyhow::Result<()> {
        rmp_serde::encode::write(&mut self.buffer, &event)?;
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
