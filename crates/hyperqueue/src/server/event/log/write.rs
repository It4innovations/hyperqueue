use crate::server::event::log::canonical_header;
use crate::server::event::{MonitoringEvent, MonitoringEventId};
use async_compression::tokio::write::GzipEncoder;
use async_compression::Level;
use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

/// Streams monitoring events into a file on disk.
pub struct EventLogWriter {
    file: File,
    buffer: Vec<u8>,

    /// The ID of the first event in current buffer.
    first_event_in_buffer: MonitoringEventId,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct EventChunkHeader {
    pub first_event_id: MonitoringEventId,
    pub chunk_length: usize,
}

pub const CHUNK_MAX_SIZE: usize = 16 * 1024;

impl EventLogWriter {
    pub async fn create(path: &Path) -> anyhow::Result<Self> {
        let mut file = File::create(path).await?;
        let header = rmp_serde::encode::to_vec(&canonical_header())?;
        file.write_all(&header).await?;
        file.flush().await?;

        let buffer = Vec::with_capacity(CHUNK_MAX_SIZE * 2);
        Ok(Self {
            file,
            buffer,
            first_event_in_buffer: 0,
        })
    }

    #[inline]
    pub async fn store(&mut self, event: MonitoringEvent) -> anyhow::Result<()> {
        if self.buffer.is_empty() {
            self.first_event_in_buffer = event.id;
        }
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

    async fn write_buffer(&mut self) -> anyhow::Result<()> {
        let mut buffer_gzipped =
            GzipEncoder::with_quality(Vec::with_capacity(CHUNK_MAX_SIZE * 2), Level::Fastest);

        let _ = buffer_gzipped.write_all(&self.buffer).await?;
        let _ = buffer_gzipped.flush().await;
        let _ = buffer_gzipped.shutdown().await;

        let chunk_header = rmp_serde::encode::to_vec(&EventChunkHeader {
            first_event_id: self.first_event_in_buffer,
            chunk_length: buffer_gzipped.get_ref().len(),
        })?;
        self.file.write_all(&chunk_header).await?;
        self.file.write_all(buffer_gzipped.get_ref()).await?;
        self.buffer.clear();
        Ok(())
    }

    #[inline]
    fn is_buffer_full(&self) -> bool {
        self.buffer.len() >= CHUNK_MAX_SIZE
    }
}
