pub mod auth;
pub mod transport;

pub type DataConnection =
    tokio_util::codec::Framed<tokio::net::TcpStream, tokio_util::codec::LengthDelimitedCodec>;
