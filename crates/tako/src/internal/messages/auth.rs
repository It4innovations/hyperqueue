use serde::{Deserialize, Serialize};
use std::borrow::Cow;

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct Challenge {
    #[serde(with = "serde_bytes")]
    pub challenge: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
//#[serde(tag = "op")]
pub(crate) enum AuthenticationMode {
    NoAuth,
    Encryption(Challenge),
}

#[derive(Serialize, Deserialize, Debug)]
//#[serde(tag = "op")]
pub(crate) struct AuthenticationRequest {
    pub protocol: u32,
    pub role: Cow<'static, str>,
    pub mode: AuthenticationMode,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct EncryptionResponse {
    #[serde(with = "serde_bytes")]
    pub response: Vec<u8>,

    #[serde(with = "serde_bytes")]
    pub nonce: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct AuthenticationError {
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug)]
//#[serde(tag = "op")]
pub(crate) enum AuthenticationResponse {
    NoAuth,
    Encryption(EncryptionResponse),
    Error(AuthenticationError),
}
