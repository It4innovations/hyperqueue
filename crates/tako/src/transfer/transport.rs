use tokio_util::codec::length_delimited::{Builder, LengthDelimitedCodec};
use tokio::net::TcpStream;
use crate::transfer::DataConnection;

pub fn make_protocol_builder() -> Builder {
    *LengthDelimitedCodec::builder()
        .little_endian()
        .max_frame_length(128 * 1024 * 1024)
}

pub async fn connect_to_worker(address: String) -> crate::Result<DataConnection> {
    let address = address.trim_start_matches("tcp://");
    let stream = TcpStream::connect(address).await?;
    stream.set_nodelay(true)?;
    Ok(make_protocol_builder().new_framed(stream))
}