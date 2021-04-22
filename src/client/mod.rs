use crate::common::error::error;
use crate::transfer::messages::ToClientMessage;

mod job;
pub mod commands;

fn handle_message(message: crate::Result<ToClientMessage>) -> crate::Result<ToClientMessage> {
    match message {
        Ok(msg) => match msg {
            ToClientMessage::Error(e) => error(format!("Server error: {}", e)),
            _ => Ok(msg)
        }
        Err(e) => error(format!("Communication error: {}", e))
    }
}
