use crate::server::event::log::{canonical_header, EventChunkHeader};
use crate::server::event::{MonitoringEvent, MonitoringEventId};
use async_compression::tokio::write::GzipEncoder;
use async_compression::Level;
use std::path::Path;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

/// Streams monitoring events into a file on disk.
pub struct EventLogWriter {
    file: File,
    state: WriterState,
    uncompressed_buffer: Vec<u8>,
}

/// The states the log writer can be in.
pub enum WriterState {
    /// The writer currently has no events in buffer.
    EmptyChunk,
    /// The writer has a chunk waiting to be written.
    ChunkInProgress(MonitoringEventId),
}

pub const CHUNK_MAX_SIZE: usize = 16 * 2048;

impl EventLogWriter {
    pub async fn create(path: &Path) -> anyhow::Result<Self> {
        let mut file = File::create(path).await?;
        let header = rmp_serde::encode::to_vec(&canonical_header())?;
        file.write_all(&header).await?;
        file.flush().await?;

        let uncompressed_buffer = Vec::with_capacity(CHUNK_MAX_SIZE * 2);
        Ok(Self {
            file,
            state: WriterState::EmptyChunk,
            uncompressed_buffer,
        })
    }

    #[inline]
    pub async fn store(&mut self, event: MonitoringEvent) -> anyhow::Result<()> {
        match self.state {
            WriterState::EmptyChunk => {
                self.set_state(WriterState::ChunkInProgress(event.id));
                self.write_event(event)?;
            }
            WriterState::ChunkInProgress(first_event_id) => {
                if self.is_buffer_full() {
                    self.write_event(event)?;

                    self.compress_and_write_buffer(first_event_id).await?;
                    self.uncompressed_buffer.clear();
                    self.set_state(WriterState::EmptyChunk);
                } else {
                    self.write_event(event)?;
                }
            }
        }
        Ok(())
    }

    pub async fn flush(&mut self) -> anyhow::Result<()> {
        if let WriterState::ChunkInProgress(first_event_id) = self.state {
            self.compress_and_write_buffer(first_event_id).await?;
        }
        self.file.flush().await?;
        Ok(())
    }

    pub async fn finish(mut self) -> anyhow::Result<()> {
        self.flush().await?;
        self.file.shutdown().await?;
        Ok(())
    }

    /// Writes event to the uncompressed_buffer
    fn write_event(&mut self, event: MonitoringEvent) -> anyhow::Result<()> {
        rmp_serde::encode::write(&mut self.uncompressed_buffer, &event)?;
        Ok(())
    }

    fn is_buffer_full(&self) -> bool {
        if self.uncompressed_buffer.len() > CHUNK_MAX_SIZE {
            return true;
        }
        false
    }

    /// Change the state of the writer
    fn set_state(&mut self, writer_state: WriterState) {
        self.state = writer_state;
    }

    /// Compresses the uncompressed_buffer and writes it to the event log file.
    async fn compress_and_write_buffer(
        &mut self,
        first_event_id: MonitoringEventId,
    ) -> anyhow::Result<()> {
        let mut buffer_gzipped =
            GzipEncoder::with_quality(Vec::with_capacity(CHUNK_MAX_SIZE * 2), Level::Fastest);

        buffer_gzipped.write_all(&self.uncompressed_buffer).await?;
        buffer_gzipped.shutdown().await?;

        let mut chunk_header = rmp_serde::encode::to_vec(&EventChunkHeader {
            first_event_id,
            chunk_length: buffer_gzipped.get_ref().len(),
        })?;
        chunk_header.append(buffer_gzipped.get_mut());
        self.file.write_all(&chunk_header).await?;
        Ok(())
    }
}
