use thiserror::Error;
use crate::common::error::HqError::GenericError;

#[derive(Debug, Error)]
pub enum HqError {
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    SerializationError(String),
    #[error("Tako error: {0}")]
    TakoError(#[from] tako::Error),
    #[error("Error: {0}")]
    GenericError(String),
}

impl From<serde_json::error::Error> for HqError {
    fn from(e: serde_json::error::Error) -> Self {
        Self::SerializationError(e.to_string())
    }
}

impl From<rmp_serde::encode::Error> for HqError {
    fn from(e: rmp_serde::encode::Error) -> Self {
        Self::SerializationError(e.to_string())
    }
}
impl From<rmp_serde::decode::Error> for HqError {
    fn from(e: rmp_serde::decode::Error) -> Self {
        Self::SerializationError(e.to_string())
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
