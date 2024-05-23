use thiserror::Error;

use crate::common::error::HqError::GenericError;

#[derive(Debug, Error)]
pub enum HqError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    SerializationError(String),
    #[error("Deserialization error: {0}")]
    DeserializationError(String),
    #[error("Tako error: {0}")]
    TakoError(#[from] tako::Error),
    #[error("Version error: {0}")]
    VersionError(String),
    #[error("Error: {0}")]
    GenericError(String),
}

impl From<serde_json::error::Error> for HqError {
    fn from(e: serde_json::error::Error) -> Self {
        Self::SerializationError(e.to_string())
    }
}

impl From<bincode::Error> for HqError {
    fn from(e: bincode::Error) -> Self {
        Self::SerializationError(e.to_string())
    }
}

impl From<anyhow::Error> for HqError {
    fn from(error: anyhow::Error) -> Self {
        Self::GenericError(error.to_string())
    }
}

impl From<toml::de::Error> for HqError {
    fn from(error: toml::de::Error) -> Self {
        Self::DeserializationError(error.to_string())
    }
}

pub fn error<T>(message: String) -> crate::Result<T> {
    Err(GenericError(message))
}

impl From<String> for HqError {
    fn from(e: String) -> Self {
        GenericError(e)
    }
}
