use std::fmt::{Debug, Display, Formatter};

use nom::character::complete::digit1;
use nom::combinator::map_res;
use nom::error::{ErrorKind, FromExternalError, ParseError};
use nom::IResult;

pub enum ParserError<I> {
    Custom(anyhow::Error),
    Nom(I, ErrorKind),
}

impl<I: Debug> Debug for ParserError<I> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Custom(error) => f.write_fmt(format_args!("Semantic error at {}", error)),
            Self::Nom(input, error) => f.write_fmt(format_args!(
                "Parser error at '{:?}': expecting {:?}",
                input, error
            )),
        }
    }
}

impl<I> ParseError<I> for ParserError<I> {
    fn from_error_kind(input: I, kind: ErrorKind) -> Self {
        ParserError::Nom(input, kind)
    }

    fn append(_: I, _: ErrorKind, other: Self) -> Self {
        other
    }
}

impl<I: Display, E: Into<anyhow::Error>> FromExternalError<I, E> for ParserError<I> {
    fn from_external_error(input: I, _: ErrorKind, error: E) -> Self {
        ParserError::Custom(anyhow::anyhow!("'{}': {}", input, error.into()))
    }
}

pub(crate) fn format_parse_error<I: Debug>(error: nom::Err<ParserError<I>>) -> anyhow::Error {
    match error {
        nom::Err::Error(e) | nom::Err::Failure(e) => anyhow::anyhow!("{:?}", e),
        _ => anyhow::anyhow!(error.to_string()),
    }
}

pub type NomResult<'a, Ret> = IResult<&'a str, Ret, ParserError<&'a str>>;

pub fn p_uint(input: &str) -> NomResult<u32> {
    map_res(digit1, |digit_str: &str| digit_str.parse::<u32>())(input)
}
