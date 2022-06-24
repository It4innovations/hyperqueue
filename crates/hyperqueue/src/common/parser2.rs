use ariadne::{Color, Fmt, Label, Report, ReportKind, Source};
use chumsky::error::Simple;
use chumsky::primitive::end;
use chumsky::Parser;

// Parsing infrastructure
pub struct ParseResult<'a, T> {
    input: &'a str,
    pub result: Result<T, Vec<Simple<char>>>,
}

impl<'a, T> ParseResult<'a, T> {
    fn new(input: &'a str, result: Result<T, Vec<Simple<char>>>) -> Self {
        Self { input, result }
    }

    pub fn into_cli_result(self) -> anyhow::Result<T> {
        self.result
            .map_err(|errors| anyhow::anyhow!("{}", format_errors_cli(self.input, errors)))
    }

    pub fn into_debug_result(self) -> anyhow::Result<T> {
        self.result
            .map_err(|errors| anyhow::anyhow!("{:?}", errors))
    }

    #[track_caller]
    pub fn unwrap(self) -> T {
        self.result.unwrap()
    }
}

pub trait CharParser<T>: Parser<char, T, Error = Simple<char>> + Sized {
    fn parse_text<'a>(&self, input: &'a str) -> ParseResult<'a, T> {
        ParseResult::new(input, self.parse(input))
    }
}
impl<T, P> CharParser<T> for P where P: Parser<char, T, Error = Simple<char>> {}

pub fn format_errors_cli(input: &str, errors: Vec<Simple<char>>) -> String {
    use std::io::Write;

    let mut error: Vec<u8> = vec![];

    for e in errors {
        let msg = if let chumsky::error::SimpleReason::Custom(msg) = e.reason() {
            msg.clone()
        } else {
            format!(
                "{}{}, expected {}",
                if e.found().is_some() {
                    "Unexpected token"
                } else {
                    "Unexpected end of input"
                },
                if let Some(label) = e.label() {
                    format!(" while parsing {}", label)
                } else {
                    String::new()
                },
                if e.expected().len() == 0 {
                    "something else".to_string()
                } else {
                    e.expected()
                        .map(|expected| match expected {
                            Some(expected) => expected.to_string(),
                            None => "end of input".to_string(),
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                },
            )
        };

        let report = Report::build(ReportKind::Error, (), e.span().start)
            .with_message(msg)
            .with_label(
                Label::new(e.span())
                    .with_message(match e.reason() {
                        chumsky::error::SimpleReason::Custom(msg) => msg.clone(),
                        _ => format!(
                            "Unexpected {}",
                            e.found()
                                .map(|c| format!("token {}", c.fg(Color::Red)))
                                .unwrap_or_else(|| "end of input".to_string())
                        ),
                    })
                    .with_color(Color::Red),
            );

        let report = match e.reason() {
            chumsky::error::SimpleReason::Unclosed { span, delimiter } => report.with_label(
                Label::new(span.clone())
                    .with_message(format!(
                        "Unclosed delimiter {}",
                        delimiter.fg(Color::Yellow)
                    ))
                    .with_color(Color::Yellow),
            ),
            chumsky::error::SimpleReason::Unexpected => report,
            chumsky::error::SimpleReason::Custom(_) => report,
        };

        if !input.is_empty() {
            report
                .finish()
                .write(Source::from(input), &mut error)
                .unwrap();
        } else {
            writeln!(&mut error, "Input is empty").unwrap();
        }
    }
    String::from_utf8(error).expect(
        "A parsing error has occurred and \
HyperQueue was unable to format it. This is a bug in HyperQueue, please report it if you see it.",
    )
}

// Common parsers
fn parse_integer_string() -> impl CharParser<String> {
    let digit = chumsky::primitive::filter(|c: &char| c.is_digit(10));
    let underscore = chumsky::primitive::just('_');
    let digit_or_underscore = underscore.or(digit.clone()).repeated();

    digit
        .chain(digit_or_underscore)
        .map(|chars| {
            chars
                .into_iter()
                .filter(|c| c.is_digit(10))
                .collect::<String>()
        })
        .labelled("number")
}

/// Parse 4-byte integer.
pub fn parse_u32() -> impl CharParser<u32> {
    parse_integer_string().try_map(|p, span| {
        p.parse::<u32>()
            .map_err(|_| Simple::custom(span, "Cannot parse as 4-byte unsigned integer"))
    })
}

/// Parse 8-byte integer.
pub fn parse_u64() -> impl CharParser<u64> {
    parse_integer_string().try_map(|p, span| {
        p.parse::<u64>()
            .map_err(|_| Simple::custom(span, "Cannot parse as 8-byte unsigned integer"))
    })
}

/// Return a parser that will fail if there is any input following the text parsed by the
/// provided parser.
pub fn all_consuming<T>(parser: impl CharParser<T>) -> impl CharParser<T> {
    parser.then_ignore(end())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_u32() {
        assert_eq!(parse_u32().parse_text("0").unwrap(), 0);
        assert_eq!(parse_u32().parse_text("1").unwrap(), 1);
        assert_eq!(parse_u32().parse_text("1019").unwrap(), 1019);
    }

    fn expect_parser_error<T: std::fmt::Debug>(parser: impl CharParser<T>, input: &str) -> String {
        let error = parser.parse_text(input).into_debug_result().unwrap_err();
        format!("{:?}", error)
    }

    #[test]
    fn test_parse_u32_empty() {
        insta::assert_debug_snapshot!(expect_parser_error(parse_u32(), ""), @r###""[Simple { span: 0..0, reason: Unexpected, expected: {}, found: None, label: Some(\"number\") }]""###);
    }

    #[test]
    fn test_parse_u32_invalid() {
        insta::assert_debug_snapshot!(expect_parser_error(parse_u32(), "x"), @r###""[Simple { span: 0..1, reason: Unexpected, expected: {}, found: Some('x'), label: Some(\"number\") }]""###);
    }

    #[test]
    fn test_parse_u32_underscores() {
        assert_eq!(parse_u32().parse_text("0_1").unwrap(), 1);
        assert_eq!(parse_u32().parse_text("1_").unwrap(), 1);
        assert_eq!(parse_u32().parse_text("100_100").unwrap(), 100100);
        assert_eq!(parse_u32().parse_text("123_456_789").unwrap(), 123456789);
    }

    #[test]
    fn test_parse_u32_repeated_underscore() {
        assert_eq!(parse_u32().parse_text("1___0__0_0").unwrap(), 1000);
    }

    #[test]
    fn test_parse_u32_starts_with_underscore() {
        insta::assert_debug_snapshot!(expect_parser_error(parse_u32(), "_"), @r###""[Simple { span: 0..1, reason: Unexpected, expected: {}, found: Some('_'), label: Some(\"number\") }]""###);
        insta::assert_debug_snapshot!(expect_parser_error(parse_u32(), "_1"), @r###""[Simple { span: 0..1, reason: Unexpected, expected: {}, found: Some('_'), label: Some(\"number\") }]""###);
    }
}
