use orion::kdf::SecretKey;
use crate::common::error::HqError::GenericError;

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
pub fn clone_key(key: &SecretKey) -> SecretKey {
    let str = serialize_key(key);
    deserialize_key(&str).unwrap()
}

#[cfg(test)]
mod tests {
    use orion::kdf::SecretKey;
    use crate::transfer::auth::{serialize_key, deserialize_key};

    #[test]
    fn test_roundtrip() {
        let key = SecretKey::default();
        let str = serialize_key(&key);
        let deserialized = deserialize_key(&str).unwrap();
        assert_eq!(key, deserialized);
    }
}
