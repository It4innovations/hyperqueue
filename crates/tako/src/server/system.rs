use serde::de::DeserializeOwned;
use serde::Serialize;

pub trait TaskSystem: Default {
    type Body: Clone + Serialize + DeserializeOwned;
}

#[derive(Default, Debug)]
pub struct DefaultTaskSystem;

impl TaskSystem for DefaultTaskSystem {
    type Body = Vec<u8>;
}
