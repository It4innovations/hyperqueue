use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct EncryptionMessage {
    #[serde(with = "serde_bytes")]
    pub nonce: Vec<u8>,

    #[serde(with = "serde_bytes")]
    pub challenge: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "op")]
pub enum AuthenticationMessage {
    NoAuth,
    Encryption(EncryptionMessage),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ChallengeResponse {
    #[serde(with = "serde_bytes")]
    pub response: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AuthenticationError {
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "op")]
pub enum AuthenticationResponse {
    NoAuth,
    ChallengeResponse(ChallengeResponse),
    Error(AuthenticationError),
}
