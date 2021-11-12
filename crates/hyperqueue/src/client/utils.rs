#[macro_export]
macro_rules! rpc_call {
    ($conn:expr, $message:expr, $matcher:pat $(=> $result:expr)?) => {
        async {
            match $conn.send_and_receive($message).await? {
                $matcher => $crate::Result::Ok(($($result),*)),
                $crate::transfer::messages::ToClientMessage::Error(e) => {
                    $crate::common::error::error(format!("Received error: {:?}", e))
                }
                msg => {
                    $crate::common::error::error(format!("Received an invalid message {:?}", msg))
                }
            }
        }
    };
}

#[macro_export]
macro_rules! arg_wrapper {
    ($name:ident, $wrapped_type:ty, $parser:expr) => {
        pub struct $name($wrapped_type);

        impl ::std::str::FromStr for $name {
            type Err = ::anyhow::Error;

            fn from_str(s: &str) -> ::std::result::Result<Self, Self::Err> {
                ::std::result::Result::Ok(Self($parser(s)?))
            }
        }

        impl $name {
            pub fn get(&self) -> &$wrapped_type {
                &self.0
            }
            pub fn unpack(self) -> $wrapped_type {
                self.0
            }
        }
    };
}
