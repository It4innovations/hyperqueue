use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use bincode::{DefaultOptions, Options};
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::{SplitSink, SplitStream};
use futures::{Sink, SinkExt};
use orion::aead::streaming::{Nonce, StreamOpener, StreamSealer, StreamTag};
use orion::kdf::SecretKey;
use orion::util::secure_rand_bytes;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::timeout;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::internal::common::error::DsError;
use crate::internal::messages::auth::{
    AuthenticationError, AuthenticationMode, AuthenticationRequest, AuthenticationResponse,
    Challenge, EncryptionResponse,
};

const CHALLENGE_LENGTH: usize = 16;

pub(crate) struct Authenticator {
    pub(crate) protocol: u32,
    pub(crate) my_role: &'static str,
    pub(crate) peer_role: &'static str,
    pub(crate) secret_key: Option<Arc<SecretKey>>,
    pub(crate) challenge: Vec<u8>,
    pub(crate) sealer: Option<StreamSealer>,
    pub(crate) error: Option<String>,
}

impl Authenticator {
    pub fn new(
        protocol: u32,
        role: &'static str,
        peer_role: &'static str,
        secret_key: Option<Arc<SecretKey>>,
    ) -> Self {
        Authenticator {
            protocol,
            my_role: role,
            peer_role,
            secret_key,
            challenge: Default::default(),
            sealer: None,
            error: None,
        }
    }

    pub fn make_auth_request(&mut self) -> crate::Result<AuthenticationRequest> {
        let mode = if self.secret_key.is_some() {
            let mut challenge = vec![0; CHALLENGE_LENGTH];
            secure_rand_bytes(&mut challenge).map_err(|_| "Generating challenge failed")?;
            self.challenge.clone_from(&challenge);
            AuthenticationMode::Encryption(Challenge { challenge })
        } else {
            AuthenticationMode::NoAuth
        };
        Ok(AuthenticationRequest {
            protocol: self.protocol,
            role: Cow::Borrowed(self.my_role),
            mode,
        })
    }

    pub(crate) fn _make_error(&mut self, message: String) -> crate::Result<AuthenticationResponse> {
        self.error = Some(message.clone());
        Ok(AuthenticationResponse::Error(AuthenticationError {
            message,
        }))
    }

    pub fn make_auth_response(
        &mut self,
        message: AuthenticationRequest,
    ) -> crate::Result<AuthenticationResponse> {
        if message.protocol != self.protocol {
            return self._make_error(format!(
                "Invalid version of protocol, expected {}, got {}",
                self.protocol, message.protocol
            ));
        }

        if message.role != self.peer_role {
            return self._make_error(format!(
                "Expected peer role {}, got {}",
                self.peer_role, message.role
            ));
        }

        match &mut (message.mode, &self.secret_key) {
            (AuthenticationMode::NoAuth, None) => Ok(AuthenticationResponse::NoAuth),
            (AuthenticationMode::Encryption(msg), Some(key)) => {
                log::debug!("Worker authorization started");
                if msg.challenge.len() != CHALLENGE_LENGTH {
                    return self._make_error(format!(
                        "Invalid length of challenge ({})",
                        msg.challenge.len()
                    ));
                }

                let (mut sealer, nonce) =
                    StreamSealer::new(key).map_err(|_| "Creating sealer failed")?;

                let mut response = Vec::new();
                response.extend_from_slice(self.my_role.as_bytes());
                response.extend_from_slice(&msg.challenge);

                let challenge_response = sealer
                    .seal_chunk(&response, &StreamTag::Message)
                    .map_err(|_| "Cannot seal challenge")?;
                self.sealer = Some(sealer);

                Ok(AuthenticationResponse::Encryption(EncryptionResponse {
                    nonce: nonce.as_ref().into(),
                    response: challenge_response,
                }))
            }
            (AuthenticationMode::Encryption(_), None) => {
                self._make_error("Peer requests authentication".to_string())
            }
            (AuthenticationMode::NoAuth, Some(_)) => {
                self._make_error("Peer does not support authentication".to_string())
            }
        }
    }

    pub fn finish_authentication(
        mut self,
        message: AuthenticationResponse,
    ) -> crate::Result<(Option<StreamSealer>, Option<StreamOpener>)> {
        if let Some(error) = std::mem::take(&mut self.error) {
            return Err(format!("Authentication failed: {error}").into());
        }

        let opener = match (message, &self.secret_key) {
            (AuthenticationResponse::Error(error), _) => {
                return Err(format!("Received authentication error: {}", error.message).into());
            }
            (AuthenticationResponse::NoAuth, None) => {
                log::debug!("Empty authentication finished");
                None
            }
            (AuthenticationResponse::Encryption(response), Some(key)) => {
                log::debug!("Challenge verification started");
                let remote_nonce =
                    &Nonce::from_slice(&response.nonce).map_err(|_| "Invalid nonce")?;
                let mut opener =
                    StreamOpener::new(key, remote_nonce).map_err(|_| "Failed to create opener")?;
                let (opened_challenge, tag) = opener
                    .open_chunk(&response.response)
                    .map_err(|_| DsError::GenericError("Cannot verify challenge".to_string()))?;

                let mut expected_response = Vec::new();
                expected_response.extend_from_slice(self.peer_role.as_bytes());
                expected_response.extend_from_slice(&self.challenge);

                if tag != StreamTag::Message || opened_challenge != expected_response {
                    return Err("Received challenge does not match.".into());
                }
                log::debug!("Challenge verification finished");
                Some(opener)
            }
            (_, _) => {
                return Err("Invalid authentication state".into());
            }
        };
        Ok((self.sealer, opener))
    }
}

pub async fn do_authentication<T: AsyncRead + AsyncWrite>(
    protocol: u32,
    my_role: &'static str,
    peer_role: &'static str,
    secret_key: Option<Arc<SecretKey>>,
    writer: &mut SplitSink<Framed<T, LengthDelimitedCodec>, bytes::Bytes>,
    reader: &mut SplitStream<Framed<T, LengthDelimitedCodec>>,
) -> crate::Result<(Option<StreamSealer>, Option<StreamOpener>)> {
    const AUTH_TIMEOUT: Duration = Duration::from_secs(15);
    let mut authenticator = Authenticator::new(protocol, my_role, peer_role, secret_key);

    /* Send authentication message */
    let message = authenticator.make_auth_request()?;
    let message_data = serialize(&message).unwrap().into();
    timeout(AUTH_TIMEOUT, writer.send(message_data))
        .await
        .map_err(|_| "Sending authentication timeout")?
        .map_err(|_| "Sending authentication failed")?;

    /* Receive authentication message */
    let remote_message_data = timeout(AUTH_TIMEOUT, reader.next())
        .await
        .map_err(|_| "Authentication message did not arrived")?
        .ok_or_else(|| {
            DsError::from("The remote side closed connection without authentication message")
        })??;
    let remote_message: AuthenticationRequest = deserialize(&remote_message_data)?;

    /* Send authentication response */
    let response = authenticator.make_auth_response(remote_message)?;
    let response_data = serialize(&response).unwrap().into();
    timeout(AUTH_TIMEOUT, writer.send(response_data))
        .await
        .map_err(|_| "Sending authentication timeouted")?
        .map_err(|_| "Sending authentication failed")?;

    /* Receive authentication response */
    let remote_response_data = timeout(AUTH_TIMEOUT, reader.next())
        .await
        .map_err(|_| "Authentication message did not arrived")?
        .ok_or_else(|| {
            DsError::from("The remote side closed connection without authentication message")
        })??;
    let remote_response: AuthenticationResponse = deserialize(&remote_response_data)?;

    // Finish authentication
    authenticator.finish_authentication(remote_response)
}

pub fn open_message<T>(opener: &mut Option<StreamOpener>, message_data: &[u8]) -> crate::Result<T>
where
    T: DeserializeOwned,
{
    if let Some(opener) = opener {
        let (msg, tag) = opener
            .open_chunk(message_data)
            .map_err(|_| DsError::GenericError("Cannot decrypt message".to_string()))?;
        assert_eq!(tag, StreamTag::Message);
        Ok(deserialize(&msg)?)
    } else {
        Ok(deserialize(message_data)?)
    }
}

#[inline]
pub fn serialize<T>(value: &T) -> crate::Result<Vec<u8>>
where
    T: serde::Serialize + ?Sized,
{
    DefaultOptions::new()
        .with_limit(crate::MAX_FRAME_SIZE as u64)
        .with_fixint_encoding()
        .serialize(value)
        .map_err(|e| format!("Serialization failed: {e:?}").into())
}

#[inline]
pub fn deserialize<'a, T>(bytes: &'a [u8]) -> crate::Result<T>
where
    T: Deserialize<'a>,
{
    DefaultOptions::new()
        .with_limit(crate::MAX_FRAME_SIZE as u64)
        .with_fixint_encoding()
        .deserialize(bytes)
        .map_err(|e| format!("Deserialization failed: {e:?}, data {bytes:?}").into())
}

#[inline]
pub fn seal_message(sealer: &mut Option<StreamSealer>, data: Bytes) -> Bytes {
    if let Some(sealer) = sealer {
        sealer
            .seal_chunk(&data, &StreamTag::Message)
            .unwrap()
            .into()
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use orion::kdf::SecretKey;

    use crate::internal::messages::auth::AuthenticationResponse;
    use crate::internal::transfer::auth::Authenticator;

    #[test]
    fn test_no_auth() {
        let mut a1 = Authenticator::new(0, "a", "b", None);
        let mut a2 = Authenticator::new(0, "b", "a", None);

        let q1 = a1.make_auth_request().unwrap();
        let q2 = a2.make_auth_request().unwrap();

        let r1 = a1.make_auth_response(q2).unwrap();
        let r2 = a2.make_auth_response(q1).unwrap();

        assert!(matches!(&r1, AuthenticationResponse::NoAuth));
        assert!(matches!(&r2, AuthenticationResponse::NoAuth));

        let (s1, o1) = a1.finish_authentication(r2).unwrap();
        let (s2, o2) = a2.finish_authentication(r1).unwrap();

        assert!(s1.is_none());
        assert!(s2.is_none());
        assert!(o1.is_none());
        assert!(o2.is_none());
    }

    #[test]
    fn test_auth_ok() {
        let secret_key = Some(Arc::new(SecretKey::generate(32).unwrap()));
        let mut a1 = Authenticator::new(0, "a", "b", secret_key.clone());
        let mut a2 = Authenticator::new(0, "b", "a", secret_key);

        let q1 = a1.make_auth_request().unwrap();
        let q2 = a2.make_auth_request().unwrap();

        let r1 = a1.make_auth_response(q2).unwrap();
        let r2 = a2.make_auth_response(q1).unwrap();

        assert!(matches!(&r1, AuthenticationResponse::Encryption(_)));
        assert!(matches!(&r2, AuthenticationResponse::Encryption(_)));

        let (s1, o1) = a1.finish_authentication(r2).unwrap();
        let (s2, o2) = a2.finish_authentication(r1).unwrap();

        assert!(s1.is_some());
        assert!(s2.is_some());
        assert!(o1.is_some());
        assert!(o2.is_some());
    }

    #[test]
    fn test_auth_different_keys() {
        let secret_key1 = Some(Arc::new(SecretKey::generate(32).unwrap()));
        let secret_key2 = Some(Arc::new(SecretKey::generate(32).unwrap()));
        let mut a1 = Authenticator::new(0, "a", "b", secret_key1);
        let mut a2 = Authenticator::new(0, "b", "a", secret_key2);

        let q1 = a1.make_auth_request().unwrap();
        let q2 = a2.make_auth_request().unwrap();

        let r1 = a1.make_auth_response(q2).unwrap();
        let r2 = a2.make_auth_response(q1).unwrap();

        assert!(matches!(&r1, AuthenticationResponse::Encryption(_)));
        assert!(matches!(&r2, AuthenticationResponse::Encryption(_)));

        assert!(a1.finish_authentication(r2).is_err());
        assert!(a2.finish_authentication(r1).is_err());
    }

    #[test]
    fn test_auth_and_no_auth() {
        let secret_key = Some(Arc::new(SecretKey::generate(32).unwrap()));
        let mut a1 = Authenticator::new(0, "a", "b", secret_key);
        let mut a2 = Authenticator::new(0, "b", "a", None);

        let q1 = a1.make_auth_request().unwrap();
        let q2 = a2.make_auth_request().unwrap();

        let r1 = a1.make_auth_response(q2).unwrap();
        let r2 = a2.make_auth_response(q1).unwrap();

        assert!(matches!(&r1, AuthenticationResponse::Error(_)));
        assert!(matches!(&r2, AuthenticationResponse::Error(_)));

        assert!(a1.finish_authentication(r2).is_err());
        assert!(a2.finish_authentication(r1).is_err());
    }

    #[test]
    fn test_mirror_attack() {
        let secret_key = Some(Arc::new(SecretKey::generate(32).unwrap()));
        let mut a1 = Authenticator::new(0, "a", "b", secret_key);

        let mut q1 = a1.make_auth_request().unwrap();
        q1.role = "b".into();
        let r1 = a1.make_auth_response(q1).unwrap();
        assert!(a1.finish_authentication(r1).is_err());
    }

    #[test]
    fn test_invalid_version() {
        let mut a1 = Authenticator::new(0, "a", "b", None);
        let mut a2 = Authenticator::new(1, "b", "a", None);

        let q1 = a1.make_auth_request().unwrap();
        let q2 = a2.make_auth_request().unwrap();

        let r1 = a1.make_auth_response(q2).unwrap();
        let r2 = a2.make_auth_response(q1).unwrap();

        assert!(matches!(&r1, AuthenticationResponse::Error(_)));
        assert!(matches!(&r2, AuthenticationResponse::Error(_)));

        assert!(a1.finish_authentication(r2).is_err());
        assert!(a2.finish_authentication(r1).is_err());
    }

    #[test]
    fn test_invalid_roles() {
        let mut a1 = Authenticator::new(0, "a", "b", None);
        let mut a2 = Authenticator::new(0, "b", "c", None);

        let q1 = a1.make_auth_request().unwrap();
        let q2 = a2.make_auth_request().unwrap();

        let r1 = a1.make_auth_response(q2).unwrap();
        let r2 = a2.make_auth_response(q1).unwrap();

        assert!(matches!(&r1, AuthenticationResponse::NoAuth));
        assert!(matches!(&r2, AuthenticationResponse::Error(_)));

        assert!(a1.finish_authentication(r2).is_err());
        assert!(a2.finish_authentication(r1).is_err());
    }
}
