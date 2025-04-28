use crate::common::arraydef::{IntArray, IntRange};
use crate::common::parser2::{all_consuming, parse_u32, CharParser, ParseError};
use chumsky::primitive::just;
use chumsky::text::TextParser;
use chumsky::Parser;
use tako::Set;

/// Parse integer range in the format n[-end][:step].
fn parse_range() -> impl CharParser<IntRange> {
    let start = parse_u32().labelled("start");
    let end = just("-").ignore_then(parse_u32()).labelled("end").or_not();
    let step = just(":").ignore_then(parse_u32()).labelled("step").or_not();

    let parser = start.then(end).then(step);
    parser
        .try_map(|((start, end), step), span| match (start, end, step) {
            (v, None, None) => Ok(IntRange::new(v, 1, 1)),
            (v, Some(w), None) if w >= v => Ok(IntRange::new(v, w - v + 1, 1)),
            (v, Some(w), Some(x)) if w >= v && x <= w - v && x > 0 => {
                Ok(IntRange::new(v, w - v + 1, x))
            }
            _ => Err(ParseError::custom(span, "Invalid range")),
        })
        .labelled("Integer range")
}

/// Parses integer ranges separated by commas.
fn parse_ranges() -> impl CharParser<Vec<IntRange>> {
    parse_range().padded().separated_by(just(',')).at_least(1)
}

/// Parses a list of integer ranges separated by commas.
/// Checks that the ranges do not overlap.
fn parse_ranges_without_overlap() -> impl CharParser<IntArray> {
    parse_ranges().try_map(|ranges, span| match ranges {
        ranges if !is_overlapping(ranges.clone()) => Ok(IntArray::new(ranges)),
        _ => Err(ParseError::custom(span, "Ranges overlap")),
    })
}

fn is_overlapping(mut ranges: Vec<IntRange>) -> bool {
    ranges.sort_unstable_by_key(|range| range.start);
    let mut ids = Set::new();
    for range in ranges {
        if range.iter().any(|x| !ids.insert(x)) {
            return true;
        }
    }
    false
}

fn parse_array_inner() -> impl CharParser<IntArray> {
    all_consuming(parse_ranges_without_overlap())
}

/// Parses integer ranges separated by commas.
/// Makes sure that the ranges do not overlap.
pub fn parse_array(input: &str) -> anyhow::Result<IntArray> {
    parse_array_inner().parse_text(input)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::tests::utils::expect_parser_error;

    #[test]
    fn test_parse_array_def() {
        assert_eq!(
            parse_array("34").unwrap().iter().collect::<Vec<_>>(),
            vec![34]
        );
        assert_eq!(
            parse_array("34-40").unwrap().iter().collect::<Vec<_>>(),
            vec![34, 35, 36, 37, 38, 39, 40]
        );
        assert_eq!(
            parse_array("101-101").unwrap().iter().collect::<Vec<_>>(),
            vec![101]
        );
        assert!(parse_array("101-100").is_err());
    }

    #[test]
    fn test_parse_arrays_def() {
        assert_eq!(
            parse_array("34,35,36").unwrap().iter().collect::<Vec<_>>(),
            vec![34, 35, 36]
        );
        assert_eq!(
            parse_array("34-40,45").unwrap().iter().collect::<Vec<_>>(),
            vec![34, 35, 36, 37, 38, 39, 40, 45]
        );
        assert_eq!(
            parse_array("0-10:2").unwrap().iter().collect::<Vec<_>>(),
            vec![0, 2, 4, 6, 8, 10]
        );
        assert!(parse_array("0-10, 5").is_err());
    }

    #[test]
    fn test_parse_array_error() {
        insta::assert_snapshot!(expect_parser_error(parse_array_inner(), "12-x"), @r###"
        Unexpected token found while attempting to parse number, expected something else:
          12-x
             |
             --- Unexpected token `x`
        "###);
    }
}
