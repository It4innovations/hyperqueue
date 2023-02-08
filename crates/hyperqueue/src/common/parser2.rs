use anyhow::anyhow;
use chumsky::error::{Simple, SimpleReason};
use chumsky::primitive::end;
use chumsky::text::ident;
use chumsky::{Error, Parser, Span};
use colored::Color;
use std::ops::Range;

#[derive(Clone, Debug, PartialEq)]
pub struct ParseError {
    error: Simple<String>,
}

impl ParseError {
    /// Create an error with a custom error message.
    pub fn custom<M: ToString>(span: <Self as Error<char>>::Span, msg: M) -> Self {
        Self {
            error: Simple::custom(span, msg),
        }
    }

    pub(crate) fn expected_input_found_string<Iter: IntoIterator<Item = Option<String>>>(
        span: <Self as Error<char>>::Span,
        expected: Iter,
        found: Option<String>,
    ) -> Self {
        Self {
            error: Simple::expected_input_found(span, expected, found),
        }
    }

    /// Returns the span that the error occured at.
    pub fn span(&self) -> <Self as Error<char>>::Span {
        self.error.span()
    }

    /// Returns an iterator over possible expected patterns.
    pub fn expected(&self) -> impl ExactSizeIterator<Item = &Option<String>> + '_ {
        self.error.expected()
    }

    /// Returns the input, if any, that was found instead of an expected pattern.
    pub fn found(&self) -> Option<&String> {
        self.error.found()
    }

    /// Returns the reason for the error.
    pub fn reason(&self) -> &SimpleReason<String, <Self as Error<char>>::Span> {
        self.error.reason()
    }

    /// Returns the error's label, if any.
    pub fn label(&self) -> Option<&'static str> {
        self.error.label()
    }
}

impl Error<char> for ParseError {
    type Span = Range<usize>;
    type Label = &'static str;

    fn expected_input_found<Iter: IntoIterator<Item = Option<char>>>(
        span: Self::Span,
        expected: Iter,
        found: Option<char>,
    ) -> Self {
        let expected = expected.into_iter().map(|c| c.map(|c| c.to_string()));
        Self {
            error: Simple::expected_input_found(span, expected, found.map(|c| c.to_string())),
        }
    }

    fn with_label(self, label: Self::Label) -> Self {
        Self {
            error: self.error.with_label(label),
        }
    }

    fn merge(self, other: Self) -> Self {
        let merged = self.error.merge(other.error);
        Self { error: merged }
    }
}

// Parsing infrastructure
pub trait CharParser<T>: Parser<char, T, Error = ParseError> + Sized + Clone {
    fn parse_text(&self, input: &str) -> anyhow::Result<T> {
        self.parse(input)
            .map_err(|errors| anyhow!("{}", format_errors_cli(input, errors)))
    }
}
impl<T, P> CharParser<T> for P where P: Parser<char, T, Error = ParseError> + Clone {}

#[cfg(not(test))]
fn color_string<S: AsRef<str>>(string: S, color: Color) -> colored::ColoredString {
    use colored::Colorize;
    string.as_ref().color(color)
}

#[cfg(test)]
fn color_string<S: AsRef<str>>(string: S, _color: Color) -> String {
    string.as_ref().to_string()
}

/// Formats `chumsky` error into a user-visible (optionally colored) string.
/// Currently it handles just the first error.
pub fn format_errors_cli(input: &str, mut errors: Vec<ParseError>) -> String {
    use std::fmt::Write;

    const ERROR_COLOR: Color = Color::Red;

    assert!(!errors.is_empty());

    errors.truncate(1);
    let error = errors.pop().unwrap();

    let mut output = String::new();

    let span = error.span();
    let expected = if error.expected().len() == 0 {
        "something else".to_string()
    } else {
        let mut expected = error
            .expected()
            .map(|expected| match expected {
                Some(expected) => expected.to_string(),
                None => "<end of input>".to_string(),
            })
            .collect::<Vec<_>>();
        expected.sort_unstable();
        expected
            .into_iter()
            .map(|expected| color_string(expected, Color::Blue).to_string())
            .collect::<Vec<_>>()
            .join(" or ")
    };

    let message = format!(
        "{} found{}, expected {}:",
        if error.found().is_some() {
            "Unexpected token"
        } else {
            "Unexpected end of input"
        },
        if let Some(label) = error.label() {
            format!(
                " while attempting to parse {}",
                color_string(label, Color::Yellow)
            )
        } else {
            String::new()
        },
        expected,
    );

    output.push_str(&message);
    output.push('\n');

    if input.is_empty() {
        output.push_str("(the input was empty)");
    } else {
        // Colored input
        writeln!(
            output,
            "  {}{}{}",
            input.chars().take(span.start()).collect::<String>(),
            color_string(
                input
                    .chars()
                    .skip(span.start())
                    .take(span.end() - span.start())
                    .collect::<String>(),
                ERROR_COLOR
            ),
            input.chars().skip(span.end()).collect::<String>()
        )
        .unwrap();

        let start_index = 2 + span.start();
        let spaces = " ".repeat(start_index);

        // Pipe
        writeln!(output, "{spaces}{}", color_string("|", ERROR_COLOR)).unwrap();

        // Note with a shortened error message
        let note = match error.reason() {
            SimpleReason::Custom(msg) => msg.clone(),
            _ => format!(
                "Unexpected {}",
                error
                    .found()
                    .map(|c| format!("token `{c}`"))
                    .unwrap_or_else(|| "end of input".to_string())
            ),
        };
        writeln!(
            output,
            "{spaces}{}{}",
            color_string("--- ", ERROR_COLOR),
            color_string(note, ERROR_COLOR)
        )
        .unwrap();
    }

    output
}

// Common parsers
fn parse_integer_string() -> impl CharParser<String> {
    let digit = chumsky::primitive::filter(|c: &char| c.is_ascii_digit());
    let underscore = chumsky::primitive::just('_');
    let digit_or_underscore = underscore.or(digit).repeated();

    digit
        .chain(digit_or_underscore)
        .map(|chars| {
            chars
                .into_iter()
                .filter(|c| c.is_ascii_digit())
                .collect::<String>()
        })
        .labelled("number")
}

/// Parse 4-byte integer.
pub fn parse_u32() -> impl CharParser<u32> {
    parse_integer_string().try_map(|p, span| {
        p.parse::<u32>()
            .map_err(|_| ParseError::custom(span, "Cannot parse as 4-byte unsigned integer"))
    })
}

/// Parse 8-byte integer.
pub fn parse_u64() -> impl CharParser<u64> {
    parse_integer_string().try_map(|p, span| {
        p.parse::<u64>()
            .map_err(|_| ParseError::custom(span, "Cannot parse as 8-byte unsigned integer"))
    })
}

/// Parses an exact string.
/// Use this instead of `chomsky::text::keyword` or `chomsky::primitive::just` for better error
/// messages if the expected string has more than a single character.
pub fn parse_exact_string(keyword: &'static str) -> impl CharParser<()> {
    ident()
        // This produces nice error even when the input is missing completely
        .map_err_with_span(|error: ParseError, span| {
            error.merge(ParseError::expected_input_found_string(
                span,
                Some(Some(keyword.to_string())),
                None,
            ))
        })
        .try_map(move |s: String, span| {
            if s.as_str() == keyword {
                Ok(())
            } else {
                Err(ParseError::expected_input_found_string(
                    span,
                    Some(Some(keyword.to_string())),
                    Some(s),
                ))
            }
        })
}

pub fn parse_named_string(name: &'static str) -> impl CharParser<String> {
    ident().map_err_with_span(|error: ParseError, span| {
        error.merge(ParseError::expected_input_found_string(
            span,
            Some(Some(name.to_string())),
            None,
        ))
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
    use crate::tests::utils::expect_parser_error;
    use chumsky::primitive::just;

    #[test]
    fn test_parse_u32() {
        assert_eq!(parse_u32().parse_text("0").unwrap(), 0);
        assert_eq!(parse_u32().parse_text("1").unwrap(), 1);
        assert_eq!(parse_u32().parse_text("1019").unwrap(), 1019);
    }

    #[test]
    fn test_parse_u32_empty() {
        insta::assert_snapshot!(expect_parser_error(parse_u32(), ""), @r###"
        Unexpected end of input found while attempting to parse number, expected something else:
        (the input was empty)
        "###);
    }

    #[test]
    fn test_parse_u32_invalid() {
        insta::assert_snapshot!(expect_parser_error(parse_u32(), "x"), @r###"
        Unexpected token found while attempting to parse number, expected something else:
          x
          |
          --- Unexpected token `x`
        "###);
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
    fn test_parse_u32_only_underscore() {
        insta::assert_snapshot!(expect_parser_error(parse_u32(), "_"), @r###"
        Unexpected token found while attempting to parse number, expected something else:
          _
          |
          --- Unexpected token `_`
        "###);
    }

    #[test]
    fn test_parse_u32_starts_with_underscore() {
        insta::assert_snapshot!(expect_parser_error(parse_u32(), "_1"), @r###"
        Unexpected token found while attempting to parse number, expected something else:
          _1
          |
          --- Unexpected token `_`
        "###);
    }

    #[test]
    fn test_parse_error_delimited_values() {
        let parser = just('x')
            .separated_by(just(','))
            .delimited_by(just('('), just(')'));

        insta::assert_snapshot!(expect_parser_error(parser, "(x,x"), @r###"
        Unexpected end of input found, expected ) or ,:
          (x,x
              |
              --- Unexpected end of input
        "###);
    }
}
