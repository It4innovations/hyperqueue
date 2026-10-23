use anyhow::Context;
use orion::kdf::SecretKey;

pub fn generate_key() -> SecretKey {
    SecretKey::generate().unwrap()
}
pub fn serialize_key(key: &SecretKey) -> String {
    let bytes = key.unprotected_as_ref::<[u8]>();
    hex::encode(bytes)
}
pub fn deserialize_key(key: &str) -> anyhow::Result<SecretKey> {
    let data = hex::decode(key).context("Could not deserialize secret key")?;
    let key = SecretKey::try_from(&data).context("Could not create secret key from slice")?;
    Ok(key)
}

#[cfg(test)]
mod tests {
    use orion::kdf::SecretKey;

    use crate::transfer::auth::{deserialize_key, serialize_key};

    #[test]
    fn test_roundtrip() {
        let key = SecretKey::generate().unwrap();
        let str = serialize_key(&key);
        let deserialized = deserialize_key(&str).unwrap();
        assert_eq!(key, deserialized);
    }
}
