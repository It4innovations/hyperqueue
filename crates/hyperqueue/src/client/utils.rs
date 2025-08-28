use clap::builder::{PossibleValue, TypedValueParser};
use clap::{Command, Error};
use std::ffi::OsStr;

#[macro_export]
macro_rules! rpc_call {
    ($conn:expr, $message:expr, $matcher:pat $(=> $result:expr)?) => {
        async {
            match $conn.send_and_receive($message).await? {
                $matcher => $crate::Result::Ok(($($result),*)),
                $crate::transfer::messages::ToClientMessage::Error(e) => {
                    $crate::common::error::error(format!("{}", e))
                }
                msg => {
                    $crate::common::error::error(format!("Received an invalid message {:?}", msg))
                }
            }
        }
    };
}

/// This argument checks that the input can be parsed as `Arg`.
/// If it is, it will return the original input from the command line as a [`String`] along with the
/// parsed value.
#[derive(Debug, Clone)]
pub struct PassThroughArgument<Arg>(String, Arg);

impl<Arg> PassThroughArgument<Arg> {
    pub fn into_original_input(self) -> String {
        self.0
    }

    pub fn as_parsed_arg(&self) -> &Arg {
        &self.1
    }

    pub fn into_parsed_arg(self) -> Arg {
        self.1
    }
}

#[derive(Clone, Debug)]
pub struct PassthroughParser<Parser>(Parser);

/// Creates a new parser that passed the original value through, while checking that `Arg`
/// can be parsed successfully.
pub fn passthrough_parser<Parser: TypedValueParser>(parser: Parser) -> PassthroughParser<Parser> {
    PassthroughParser(parser)
}

impl<Parser, Arg> TypedValueParser for PassthroughParser<Parser>
where
    Parser: TypedValueParser<Value = Arg>,
    Arg: Clone + Sync + Send + 'static,
{
    type Value = PassThroughArgument<Arg>;

    fn parse_ref(
        &self,
        cmd: &Command,
        arg: Option<&clap::Arg>,
        value: &OsStr,
    ) -> Result<Self::Value, Error> {
        self.0
            .parse_ref(cmd, arg, value)
            .map(|parsed| PassThroughArgument(value.to_string_lossy().to_string(), parsed))
    }

    fn possible_values(&self) -> Option<Box<dyn Iterator<Item = PossibleValue> + '_>> {
        self.0.possible_values()
    }
}
