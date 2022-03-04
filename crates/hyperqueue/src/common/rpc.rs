use std::fmt::{Debug, Formatter};
use tokio::sync::{mpsc, oneshot};

/// Can be used to respond to a RPC call.
#[must_use = "response token should be used to respond to a request"]
pub struct ResponseToken<T> {
    sender: oneshot::Sender<T>,
}

impl<T> Debug for ResponseToken<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("Response token")
    }
}

impl<T> ResponseToken<T> {
    pub fn respond(self, response: T) {
        if let Err(_e) = self.sender.send(response) {
            log::warn!("Could not send response to RPC method, the other end hang up");
        }
    }
}

/// Helper function for creating request-response RPC calls.
/// Expects a callback that will receive a response token.
/// This function will return once the response token has been resolved.
pub fn initiate_request<F, Response, R>(make_request: F) -> oneshot::Receiver<Response>
where
    F: FnOnce(ResponseToken<Response>) -> Result<(), mpsc::error::SendError<R>>,
    R: std::fmt::Debug,
{
    let (tx, rx) = oneshot::channel::<Response>();
    let token = ResponseToken { sender: tx };
    if let Err(error) = make_request(token) {
        log::warn!("Could not make RPC request: {error:?}");
    }
    rx
}

pub type RpcSender<T> = mpsc::UnboundedSender<T>;
pub type RpcReceiver<T> = mpsc::UnboundedReceiver<T>;

pub fn make_rpc_queue<T>() -> (RpcSender<T>, RpcReceiver<T>) {
    mpsc::unbounded_channel()
}
