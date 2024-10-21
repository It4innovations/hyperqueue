use bincode::Options;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;

/// Helper trait to configure serialization options via separate types.
pub trait SerializationConfig {
    fn config() -> impl Options;
}

pub struct DefaultConfig;

impl SerializationConfig for DefaultConfig {
    fn config() -> impl Options {
        bincode::DefaultOptions::new()
    }
}

pub struct TrailingAllowedConfig;

impl SerializationConfig for TrailingAllowedConfig {
    fn config() -> impl Options {
        bincode::DefaultOptions::new().allow_trailing_bytes()
    }
}

/// Strongly typed wrapper over `<T>` serialized with Bincode.
#[derive(Serialize, Deserialize)]
pub struct Serialized<T, C = DefaultConfig> {
    #[serde(with = "serde_bytes")]
    data: Vec<u8>,
    _phantom: PhantomData<(T, C)>,
}

impl<T, C> Debug for Serialized<T, C> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Serialized {} ({}) byte(s)",
            std::any::type_name::<T>(),
            self.data.len()
        )
    }
}

impl<T, C> Clone for Serialized<T, C> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T: Serialize + DeserializeOwned, C: SerializationConfig> Serialized<T, C> {
    pub fn new(value: &T) -> bincode::Result<Self> {
        Ok(Self {
            data: C::config().serialize(value)?,
            _phantom: Default::default(),
        })
    }

    pub fn deserialize(&self) -> bincode::Result<T> {
        C::config().deserialize(&self.data)
    }
}
