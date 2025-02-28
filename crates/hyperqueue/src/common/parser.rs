use std::error::Error;
use std::fmt::{Debug, Write};

use nom::character::complete::satisfy;
use nom::combinator::{all_consuming, map, map_res};
use nom::error::{ErrorKind, FromExternalError, ParseError};
use nom::multi::many0;
use nom::sequence::tuple;
use nom::{AsChar, IResult, InputLength, Parser};
use nom_supreme::ParserExt;
use nom_supreme::error::{BaseErrorKind, ErrorTree, Expectation, StackContext};
use nom_supreme::final_parser::{ByteOffset, RecreateContext};
use nom_supreme::tag::TagError;

const CONTEXT_INTEGER: &str = "integer";

// Parser implementation details below these contexts will not be shown to the user
const TERMINAL_CONTEXTS: [&str; 1] = [CONTEXT_INTEGER];

#[derive(Debug)]
pub struct ParserError<I>(ErrorTree<I>);

impl<I> From<ErrorTree<I>> for ParserError<I> {
    fn from(error: ErrorTree<I>) -> Self {
        ParserError(error)
    }
}

impl<I> TagError<I, &'static str> for ParserError<I> {
    fn from_tag(input: I, tag: &'static str) -> Self {
        ErrorTree::from_tag(input, tag).into()
    }

    fn from_tag_no_case(input: I, tag: &'static str) -> Self {
        ErrorTree::from_tag_no_case(input, tag).into()
    }
}

impl<I> nom_supreme::context::ContextError<I, &'static str> for ParserError<I> {
    fn add_context(location: I, ctx: &'static str, other: Self) -> Self {
        <ErrorTree<I> as nom_supreme::context::ContextError<I, &'static str>>::add_context(
            location, ctx, other.0,
        )
        .into()
    }
}

impl<I: InputLength> ParseError<I> for ParserError<I> {
    fn from_error_kind(input: I, kind: ErrorKind) -> Self {
        ErrorTree::from_error_kind(input, kind).into()
    }

    fn append(input: I, error: ErrorKind, other: Self) -> Self {
        ErrorTree::append(input, error, other.0).into()
    }

    fn from_char(input: I, c: char) -> Self {
        ErrorTree::from_char(input, c).into()
    }

    fn or(self, other: Self) -> Self {
        ErrorTree::or(self.0, other.0).into()
    }
}

impl<I, E: Into<anyhow::Error>> FromExternalError<I, E> for ParserError<I> {
    fn from_external_error(input: I, _kind: ErrorKind, error: E) -> Self {
        ParserError(ErrorTree::Base {
            location: input,
            kind: BaseErrorKind::External(error.into().into()),
        })
    }
}

fn format_kind(
    kind: BaseErrorKind<&'static str, Box<dyn Error + Send + Sync + 'static>>,
) -> String {
    match kind {
        BaseErrorKind::Expected(expectation) => match expectation {
            Expectation::Tag(tag) => format!(r#"expected "{tag}""#),
            Expectation::Char(c) => format!(r#"expected "{c}""#),
            Expectation::Alpha => "expected alphabet character".to_string(),
            Expectation::Digit | Expectation::HexDigit | Expectation::OctDigit => {
                "expected digit".to_string()
            }
            Expectation::AlphaNumeric => "expected alphanumeric character".to_string(),
            Expectation::Space | Expectation::Multispace | Expectation::CrLf => {
                "expected whitespace".to_string()
            }
            Expectation::Eof => "expected end of input".to_string(),
            _ => "expected something".to_string(),
        },
        BaseErrorKind::Kind(kind) => format!("expected: {kind:?}"),
        BaseErrorKind::External(error) => format!(r#""{error:?}""#),
    }
}

fn format_location(input: &str, location: &str) -> String {
    if location.is_empty() {
        "the end of input".to_string()
    } else {
        let offset = ByteOffset::recreate_context(input, location).0;
        format!("character {offset}: {location:?}")
    }
}

fn indent(depth: u32) -> String {
    " ".repeat((depth as usize) * 2)
}

fn format_error(error: ErrorTree<&str>, mut depth: u32, input: &str, buffer: &mut String) {
    match error {
        ErrorTree::Base { location, kind } => {
            buffer
                .write_fmt(format_args!(
                    "{}{} at {}\n",
                    indent(depth),
                    format_kind(kind),
                    format_location(input, location)
                ))
                .unwrap();
        }
        ErrorTree::Stack { base, contexts } => {
            let mut show_nested = true;
            for (location, ctx) in contexts.into_iter().rev() {
                let ctx_str = match ctx {
                    StackContext::Kind(kind) => format!("{kind:?}"),
                    StackContext::Context(ctx) => {
                        if TERMINAL_CONTEXTS.contains(&ctx) {
                            show_nested = false;
                        }

                        ctx.to_string()
                    }
                };
                buffer
                    .write_fmt(format_args!(
                        "{}expected {} at {}",
                        indent(depth),
                        ctx_str,
                        format_location(input, location)
                    ))
                    .unwrap();
                buffer.push('\n');
            }
            if show_nested {
                format_error(*base, depth + 1, input, buffer);
            }
        }
        ErrorTree::Alt(mut choices) => {
            // If an external error (`map_res`) has happened, it should have higher priority
            // than other alternatives.
            let external_error_index = choices.iter().position(|c| {
                matches!(
                    c,
                    ErrorTree::Base {
                        kind: BaseErrorKind::External(_),
                        ..
                    }
                )
            });

            match external_error_index {
                Some(index) => format_error(choices.swap_remove(index), depth, input, buffer),
                None => {
                    let length = choices.len();
                    buffer
                        .write_fmt(format_args!(
                            "{}expected one of the following {} variants:\n",
                            indent(depth),
                            length
                        ))
                        .unwrap();

                    depth += 1;

                    for (index, choice) in choices.into_iter().enumerate() {
                        format_error(choice, depth, input, buffer);
                        if index != length - 1 {
                            buffer.push_str(&indent(depth));
                            buffer.push_str("or\n");
                        }
                    }
                }
            }
        }
    }
}

pub fn format_parse_error(error: nom::Err<ParserError<&str>>, input: &str) -> anyhow::Error {
    match error {
        nom::Err::Error(e) | nom::Err::Failure(e) => {
            let mut buffer = "Parse error\n".to_string();
            format_error(e.0, 0, input, &mut buffer);
            anyhow::anyhow!("{}", buffer.trim_end())
        }
        nom::Err::Incomplete(needed) => anyhow::anyhow!("Incomplete input (needed: {:?})", needed),
    }
}

pub type NomResult<'a, Ret> = IResult<&'a str, Ret, ParserError<&'a str>>;

pub fn map_parse_result<O>(
    result: Result<O, nom::Err<ParserError<&str>>>,
    input: &str,
) -> anyhow::Result<O> {
    result.map_err(|e| format_parse_error(e, input))
}

/// Take a parser and input and return Result with a formatted error.
pub fn consume_all<'a, O, F>(f: F, input: &'a str) -> anyhow::Result<O>
where
    F: FnMut(&'a str) -> NomResult<'a, O>,
{
    map_parse_result(all_consuming(f)(input).map(|r| r.1), input)
}

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

/// Parse 4 byte integer.
pub fn p_u32(input: &str) -> NomResult<u32> {
    map_res(p_integer_string, |number| number.parse())
        .context(CONTEXT_INTEGER)
        .parse(input)
}

/// Parse 8 byte integer
pub fn p_u64(input: &str) -> NomResult<u64> {
    map_res(p_integer_string, |number| number.parse())
        .context(CONTEXT_INTEGER)
        .parse(input)
}

#[cfg(test)]
mod tests {
    use nom::combinator::all_consuming;

    use crate::tests::utils::check_parse_error;

    use super::p_u32;

    #[test]
    fn test_parse_u32() {
        assert_eq!(all_consuming(p_u32)("0").unwrap().1, 0);
        assert_eq!(all_consuming(p_u32)("1").unwrap().1, 1);
        assert_eq!(all_consuming(p_u32)("1019").unwrap().1, 1019);
    }

    #[test]
    fn test_parse_u32_empty() {
        check_parse_error(
            p_u32,
            "",
            r#"Parse error
expected integer at the end of input"#,
        );
    }

    #[test]
    fn test_parse_u32_invalid() {
        check_parse_error(
            p_u32,
            "x",
            r#"Parse error
expected integer at character 0: "x""#,
        );
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
        check_parse_error(
            p_u32,
            "_",
            r#"Parse error
expected integer at character 0: "_""#,
        );
        check_parse_error(
            p_u32,
            "_1",
            r#"Parse error
expected integer at character 0: "_1""#,
        );
    }
}
