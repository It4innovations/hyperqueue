use std::fs::File;
use std::io::Read;
use std::path::Path;

use orion::aead::SecretKey;

use crate::common::error::DsError;

pub fn read_secret_file(path: &Path) -> crate::Result<SecretKey> {
    log::info!("Reading secret file from file '{}'", path.display());
    let mut secret_file = File::open(&path)?;

    let mut hex_secret: [u8; 64] = [0; 64];
    match secret_file.read(&mut hex_secret) {
        Ok(64) => { /* Ok */ }
        _ => {
            return Err(DsError::GenericError(format!(
                "Invalid length of secret file"
            )));
        }
    }
    Ok(SecretKey::from_slice(
        &hex::decode(&hex_secret)
            .map_err(|_| DsError::GenericError(format!("Invalid content of the secret file")))?,
    )
    .unwrap())
}
