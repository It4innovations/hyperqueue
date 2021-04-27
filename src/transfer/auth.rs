use crate::common::error::HqError::GenericError;
use orion::kdf::SecretKey;

const KEY_SIZE: usize = 32;

pub fn generate_key() -> SecretKey {
    SecretKey::generate(KEY_SIZE).unwrap()
}
pub fn serialize_key(key: &SecretKey) -> String {
    let bytes = key.unprotected_as_bytes();
    hex::encode(bytes)
}
pub fn deserialize_key(key: &str) -> crate::Result<SecretKey> {
    let data = hex::decode(key)
        .map_err(|e| GenericError(format!("Could not deserialize secret key: {}", e)))?;
    let key = SecretKey::from_slice(&data)
        .map_err(|e| GenericError(format!("Could not create secret key from slice: {}", e)))?;
    Ok(key)
}

#[cfg(test)]
mod tests {
    use crate::transfer::auth::{deserialize_key, serialize_key};
    use orion::kdf::SecretKey;

    #[test]
    fn test_roundtrip() {
        let key = SecretKey::default();
        let str = serialize_key(&key);
        let deserialized = deserialize_key(&str).unwrap();
        assert_eq!(key, deserialized);
    }
}
