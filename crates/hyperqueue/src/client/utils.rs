use std::marker::PhantomData;
use std::str::FromStr;

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

/// This argument checks that the input can be parsed as `Arg`.
/// If it is, it will return the original input from the command line as a [`String`].
pub struct PassThroughArgument<Arg>(String, PhantomData<Arg>);

impl<Arg: FromStr> FromStr for PassThroughArgument<Arg> {
    type Err = <Arg as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Arg::from_str(s)?;
        Ok(Self(s.to_string(), Default::default()))
    }
}

impl<Arg> From<PassThroughArgument<Arg>> for String {
    fn from(arg: PassThroughArgument<Arg>) -> Self {
        arg.0
    }
}
