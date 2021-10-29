use std::fmt::{Debug, Display, Formatter};

use nom::character::complete::satisfy;
use nom::combinator::{map, map_res};
use nom::error::{ErrorKind, FromExternalError, ParseError};
use nom::multi::many0;
use nom::sequence::tuple;
use nom::{AsChar, IResult};

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

fn p_integer_string(input: &str) -> NomResult<String> {
    let parser = tuple((
        satisfy(|c| c.is_dec_digit()),
        many0(satisfy(|c| c.is_dec_digit() || c == '_')),
    ));
    map(parser, |(first, rest)| {
        let mut number = first.to_string();
        number.extend(rest.into_iter().filter(|c| c.is_dec_digit()));
        number
    })(input)
}

pub fn p_u32(input: &str) -> NomResult<u32> {
    map_res(p_integer_string, |number| number.parse::<u32>())(input)
}

pub fn p_u64(input: &str) -> NomResult<u64> {
    map_res(p_integer_string, |number| number.parse::<u64>())(input)
}

#[cfg(test)]
mod tests {
    use super::p_u32;
    use nom::combinator::all_consuming;

    #[test]
    fn test_parse_u32() {
        assert_eq!(all_consuming(p_u32)("0").unwrap().1, 0);
        assert_eq!(all_consuming(p_u32)("1").unwrap().1, 1);
        assert_eq!(all_consuming(p_u32)("1019").unwrap().1, 1019);
    }

    #[test]
    fn test_parse_u32_empty() {
        assert!(all_consuming(p_u32)("").is_err());
    }

    #[test]
    fn test_parse_u32_invalid() {
        assert!(all_consuming(p_u32)("x").is_err());
    }

    #[test]
    fn test_parse_u32_underscores() {
        assert_eq!(all_consuming(p_u32)("0_1").unwrap().1, 1);
        assert_eq!(all_consuming(p_u32)("1_").unwrap().1, 1);
        assert_eq!(all_consuming(p_u32)("100_100").unwrap().1, 100100);
        assert_eq!(all_consuming(p_u32)("123_456_789").unwrap().1, 123456789);
    }

    #[test]
    fn test_parse_u32_repeated_underscore() {
        assert_eq!(all_consuming(p_u32)("1___0__0_0").unwrap().1, 1000);
    }

    #[test]
    fn test_parse_u32_starts_with_underscore() {
        assert!(all_consuming(p_u32)("_").is_err());
        assert!(all_consuming(p_u32)("_1").is_err());
    }
}
