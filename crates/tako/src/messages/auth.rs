use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Challenge {
    #[serde(with = "serde_bytes")]
    pub challenge: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "op")]
pub enum AuthenticationMode {
    NoAuth,
    Encryption(Challenge),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "op")]
pub struct AuthenticationRequest {
    pub protocol: u32,
    pub role: String,
    pub mode: AuthenticationMode,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct EncryptionResponse {
    #[serde(with = "serde_bytes")]
    pub response: Vec<u8>,

    #[serde(with = "serde_bytes")]
    pub nonce: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AuthenticationError {
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "op")]
pub enum AuthenticationResponse {
    NoAuth,
    Encryption(EncryptionResponse),
    Error(AuthenticationError),
}
