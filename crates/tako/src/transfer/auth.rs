use std::rc::Rc;
use std::time::Duration;

use bytes::Bytes;
use futures::stream::{SplitSink, SplitStream};
use futures::StreamExt;
use futures::{Sink, SinkExt};
use orion::aead::streaming::{Nonce, StreamOpener, StreamSealer, StreamTag};
use orion::kdf::SecretKey;
use orion::util::secure_rand_bytes;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::timeout;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::common::error::DsError;
use crate::messages::auth::{
    AuthenticationError, AuthenticationMessage, AuthenticationResponse, ChallengeResponse,
    EncryptionMessage,
};

const CHALLENGE_LENGTH: usize = 20;

struct Authenticator {
    pub secret_key: Option<Rc<SecretKey>>,
    pub challenge: Vec<u8>,
    pub sealer: Option<StreamSealer>,
    pub opener: Option<StreamOpener>,
    pub error: Option<String>,
}

impl Authenticator {
    pub fn new(secret_key: Option<Rc<SecretKey>>) -> Self {
        Authenticator {
            secret_key,
            challenge: Default::default(),
            sealer: None,
            opener: None,
            error: None,
        }
    }

    pub fn make_auth_message(&mut self) -> crate::Result<AuthenticationMessage> {
        if let Some(key) = &self.secret_key {
            let mut challenge = vec![0; CHALLENGE_LENGTH];
            secure_rand_bytes(&mut challenge).unwrap();
            let (sealer, nonce) = StreamSealer::new(&key).unwrap();
            self.sealer = Some(sealer);
            self.challenge = challenge.clone();
            Ok(AuthenticationMessage::Encryption(EncryptionMessage {
                nonce: nonce.as_ref().into(),
                challenge,
            }))
        } else {
            Ok(AuthenticationMessage::NoAuth)
        }
    }

    pub fn make_auth_response(
        &mut self,
        message: AuthenticationMessage,
    ) -> crate::Result<AuthenticationResponse> {
        match &mut (message, &mut self.sealer) {
            (AuthenticationMessage::NoAuth, None) => Ok(AuthenticationResponse::NoAuth),
            (AuthenticationMessage::Encryption(msg), Some(ref mut sealer)) => {
                log::debug!("Worker authorization started");
                let key = self.secret_key.as_ref().unwrap();
                if msg.challenge.len() != CHALLENGE_LENGTH {
                    return Err(DsError::GenericError(format!(
                        "Invalid length of challenge ({})",
                        msg.challenge.len()
                    )));
                }
                let remote_nonce = &Nonce::from_slice(&msg.nonce)
                    .map_err(|_| DsError::GenericError("Invalid nonce".to_string()))?;
                let opener = StreamOpener::new(key, remote_nonce)
                    .map_err(|_| DsError::GenericError("Failed to create opener".to_string()))?;
                self.opener = Some(opener);
                let challenge_response = sealer
                    .seal_chunk(&msg.challenge, StreamTag::Message)
                    .map_err(|_| DsError::GenericError("Cannot seal challenge".to_string()))?;
                Ok(AuthenticationResponse::ChallengeResponse(
                    ChallengeResponse {
                        response: challenge_response,
                    },
                ))
            }
            (AuthenticationMessage::Encryption(_), None) => {
                let msg = "Peer requests authentication".to_string();
                self.error = Some(msg.clone());
                Ok(AuthenticationResponse::Error(AuthenticationError {
                    message: msg,
                }))
            }
            (AuthenticationMessage::NoAuth, Some(_)) => {
                let msg = "Peer does not support authentication".to_string();
                self.error = Some(msg.clone());
                Ok(AuthenticationResponse::Error(AuthenticationError {
                    message: msg,
                }))
            }
        }
    }

    pub fn finish_authentication(
        mut self,
        message: AuthenticationResponse,
    ) -> crate::Result<(Option<StreamSealer>, Option<StreamOpener>)> {
        if let Some(error) = std::mem::take(&mut self.error) {
            return Err(DsError::GenericError(format!(
                "Authentication failed: {}",
                error
            )));
        }

        match (message, &mut self.opener) {
            (AuthenticationResponse::Error(error), _) => {
                return Err(DsError::GenericError(format!(
                    "Received authentication error: {}",
                    error.message
                )));
            }
            (AuthenticationResponse::NoAuth, None) => {
                log::debug!("Empty authentication finished");
            }
            (AuthenticationResponse::ChallengeResponse(response), Some(ref mut opener)) => {
                log::debug!("Challenge verification started");
                let (opened_challenge, tag) = opener
                    .open_chunk(&response.response)
                    .map_err(|_| DsError::GenericError("Cannot verify challenge".to_string()))?;
                if tag != StreamTag::Message || opened_challenge != self.challenge {
                    return Err(DsError::GenericError(format!(
                        "Received challenge does not match. Replay attack?"
                    )));
                }
                log::debug!("Challenge verification finished");
            }
            (_, _) => {
                return Err(DsError::GenericError(format!(
                    "Invalid authentication state"
                )));
            }
        }
        Ok((self.sealer, self.opener))
    }
}

pub async fn do_authentication<T: AsyncRead + AsyncWrite>(
    secret_key: Option<Rc<SecretKey>>,
    writer: &mut SplitSink<Framed<T, LengthDelimitedCodec>, bytes::Bytes>,
    reader: &mut SplitStream<Framed<T, LengthDelimitedCodec>>,
) -> crate::Result<(Option<StreamSealer>, Option<StreamOpener>)> {
    const AUTH_TIMEOUT: Duration = Duration::from_secs(15);
    let mut authenticator = Authenticator::new(secret_key);

    /* Send authentication message */
    let message = authenticator.make_auth_message()?;
    let message_data = rmp_serde::to_vec_named(&message).unwrap().into();
    timeout(AUTH_TIMEOUT, writer.send(message_data))
        .await
        .map_err(|_| DsError::GenericError("Sending authentication timeouted".to_string()))?
        .map_err(|_| DsError::GenericError("Sending authentication failed".to_string()))?;

    /* Receive authentication message */
    let remote_message_data = timeout(AUTH_TIMEOUT, reader.next())
        .await
        .map_err(|_| DsError::GenericError("Authentication message did not arrived".to_string()))?
        .ok_or_else(|| {
            DsError::GenericError(
                "The remote side closed connection without authentication message".to_string(),
            )
        })??;
    let remote_message: AuthenticationMessage = rmp_serde::from_slice(&remote_message_data)?;

    /* Send authentication response */
    let response = authenticator.make_auth_response(remote_message)?;
    let response_data = rmp_serde::to_vec_named(&response).unwrap().into();
    timeout(AUTH_TIMEOUT, writer.send(response_data))
        .await
        .map_err(|_| DsError::GenericError("Sending authentication timeouted".to_string()))?
        .map_err(|_| DsError::GenericError("Sending authentication failed".to_string()))?;

    /* Receive authentication response */
    let remote_response_data = timeout(AUTH_TIMEOUT, reader.next())
        .await
        .map_err(|_| DsError::GenericError("Authentication message did not arrived".to_string()))?
        .ok_or_else(|| {
            DsError::GenericError(
                "The remote side closed connection without authentication message".to_string(),
            )
        })??;
    let remote_response: AuthenticationResponse = rmp_serde::from_slice(&remote_response_data)?;

    // Finish authentication
    authenticator.finish_authentication(remote_response)
}

pub fn open_message<T>(opener: &mut Option<StreamOpener>, message_data: &[u8]) -> crate::Result<T>
where
    T: DeserializeOwned,
{
    if let Some(opener) = opener {
        let (msg, tag) = opener
            .open_chunk(&message_data)
            .map_err(|_| DsError::GenericError("Cannot decrypt message".to_string()))?;
        assert_eq!(tag, StreamTag::Message);
        Ok(rmp_serde::from_slice(&msg)?)
    } else {
        Ok(rmp_serde::from_slice(&message_data)?)
    }
}

#[inline]
pub fn seal_message(sealer: &mut Option<StreamSealer>, data: Bytes) -> Bytes {
    if let Some(sealer) = sealer {
        sealer.seal_chunk(&data, StreamTag::Message).unwrap().into()
    } else {
        data
    }
}

pub async fn forward_queue_to_sealed_sink<E, S: Sink<Bytes, Error = E> + Unpin>(
    mut queue: UnboundedReceiver<Bytes>,
    mut sink: S,
    mut sealer: Option<StreamSealer>,
) -> Result<(), E> {
    while let Some(data) = queue.recv().await {
        if let Err(e) = sink.send(seal_message(&mut sealer, data)).await {
            log::debug!("Forwarding from queue failed");
            return Err(e);
        }
    }
    Ok(())
}
