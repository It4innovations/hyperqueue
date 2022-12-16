use clap::builder::TypedValueParser;
use clap::{Command, Error};
use std::ffi::OsStr;
use std::marker::PhantomData;

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

/// This argument checks that the input can be parsed as `Arg`.
/// If it is, it will return the original input from the command line as a [`String`].
#[derive(Debug, Clone)]
pub struct PassThroughArgument<Arg>(String, PhantomData<Arg>);

impl<Arg> From<PassThroughArgument<Arg>> for String {
    fn from(arg: PassThroughArgument<Arg>) -> Self {
        arg.0
    }
}

#[derive(Clone)]
pub struct PassthroughParser<Arg>(fn(&str) -> anyhow::Result<Arg>);

/// Creates a new parser that passed the original value through, while checking that `Arg`
/// can be parsed successfully.
pub fn passthrough_parser<Arg>(parser: fn(&str) -> anyhow::Result<Arg>) -> PassthroughParser<Arg> {
    PassthroughParser(parser)
}

impl<Arg: Clone + Sync + Send + 'static> TypedValueParser for PassthroughParser<Arg> {
    type Value = PassThroughArgument<Arg>;

    fn parse_ref(
        &self,
        cmd: &Command,
        arg: Option<&clap::Arg>,
        value: &OsStr,
    ) -> Result<Self::Value, Error> {
        self.0
            .parse_ref(cmd, arg, value)
            .map(|_| PassThroughArgument(value.to_string_lossy().to_string(), Default::default()))
    }
}
