use orion::kdf::SecretKey;

use anyhow::Context;

const KEY_SIZE: usize = 32;

pub fn generate_key() -> SecretKey {
    SecretKey::generate(KEY_SIZE).unwrap()
}
pub fn serialize_key(key: &SecretKey) -> String {
    let bytes = key.unprotected_as_bytes();
    hex::encode(bytes)
}
pub fn deserialize_key(key: &str) -> anyhow::Result<SecretKey> {
    let data = hex::decode(key).context("Could not deserialize secret key")?;
    let key = SecretKey::from_slice(&data).context("Could not create secret key from slice")?;
    Ok(key)
}

#[cfg(test)]
mod tests {
    use orion::kdf::SecretKey;

    use crate::transfer::auth::{deserialize_key, serialize_key};

    #[test]
    fn test_roundtrip() {
        let key = SecretKey::default();
        let str = serialize_key(&key);
        let deserialized = deserialize_key(&str).unwrap();
        assert_eq!(key, deserialized);
    }
}
