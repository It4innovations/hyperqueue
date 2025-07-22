use bstr::BString;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub(crate) struct Notification {
    pub message: BString,
}
