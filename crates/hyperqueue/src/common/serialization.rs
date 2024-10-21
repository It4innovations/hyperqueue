use bincode::Options;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

#[inline]
pub(crate) fn bincode_config() -> impl Options {
    bincode::DefaultOptions::new().allow_trailing_bytes()
}

/// Strongly typed wrapper over <T> serialized with Bincode.
#[derive(Serialize, Deserialize, Debug)]
pub struct Serialized<T: Serialize + DeserializeOwned> {
    #[serde(with = "serde_bytes")]
    data: Vec<u8>,
    _phantom: PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned> Clone for Serialized<T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T: Serialize + DeserializeOwned> Serialized<T> {
    pub fn new(value: &T) -> bincode::Result<Self> {
        Ok(Self {
            data: bincode_config().serialize(value)?,
            _phantom: Default::default(),
        })
    }

    pub fn deserialize(&self) -> bincode::Result<T> {
        bincode_config().deserialize(&self.data)
    }
}
