use tokio_util::codec::length_delimited::{Builder, LengthDelimitedCodec};

pub fn make_protocol_builder() -> Builder {
    *LengthDelimitedCodec::builder()
        .little_endian()
        .max_frame_length(128 * 1024 * 1024)
}
