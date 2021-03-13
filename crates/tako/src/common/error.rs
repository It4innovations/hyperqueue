use thiserror::Error;

#[derive(Debug, Error)]
pub enum DsError {
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    SerializationError(String),
    #[error("Scheduler error: {0}")]
    SchedulerError(String),
    #[error("Error: {0}")]
    GenericError(String),
}

impl From<serde_json::error::Error> for DsError {
    fn from(e: serde_json::error::Error) -> Self {
        Self::SerializationError(e.to_string())
    }
}
impl From<rmp_serde::encode::Error> for DsError {
    fn from(e: rmp_serde::encode::Error) -> Self {
        Self::SerializationError(e.to_string())
    }
}
impl From<rmp_serde::decode::Error> for DsError {
    fn from(e: rmp_serde::decode::Error) -> Self {
        Self::SerializationError(e.to_string())
    }
}
