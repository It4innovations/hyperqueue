use chumsky::Parser;
use chumsky::primitive::{choice, filter, just};
use chumsky::text::TextParser;

use tako::resources::AllocationRequest;

use crate::common::parser2::{
    CharParser, ParseError, all_consuming, parse_exact_string, parse_resource_amount,
};
use crate::worker::parser::{is_valid_resource_char, is_valid_starting_resource_char};

fn p_allocation_request() -> impl CharParser<AllocationRequest> {
    parse_exact_string("all")
        .map(|_| AllocationRequest::All)
        .or(parse_resource_amount()
            .padded()
            .then(choice((just("compact!"), just("compact"), just("scatter"))).or_not())
            .try_map(|(amount, policy), span| {
                let alloc = match policy {
                    None | Some("compact") => AllocationRequest::Compact(amount),
                    Some("compact!") => AllocationRequest::ForceCompact(amount),
                    Some("scatter") => AllocationRequest::Scatter(amount),
                    _ => unreachable!(),
                };
                alloc
                    .validate()
                    .map_err(|e| ParseError::custom(span, e.to_string()))?;
                Ok(alloc)
            }))
}

fn p_resource_identifier() -> impl CharParser<String> {
    filter(|&c| is_valid_starting_resource_char(c))
        .chain(filter(|&c| is_valid_resource_char(c)).repeated())
        .map(|s| s.iter().collect::<String>())
}

fn p_resource_request() -> impl CharParser<(String, AllocationRequest)> {
    p_resource_identifier()
        .then_ignore(just("=").padded())
        .then(p_allocation_request())
}

pub fn parse_resource_request(input: &str) -> anyhow::Result<(String, AllocationRequest)> {
    all_consuming(p_resource_request()).parse_text(input)
}

pub fn parse_allocation_request(input: &str) -> anyhow::Result<AllocationRequest> {
    all_consuming(p_allocation_request()).parse_text(input)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::tests::utils::expect_parser_error;
    use tako::resources::ResourceAmount;

    #[test]
    fn test_parse_resource_request() {
        assert_eq!(
            parse_resource_request("xxx=all").unwrap(),
            ("xxx".to_string(), AllocationRequest::All)
        );
        assert_eq!(
            parse_resource_request("ab1c=10").unwrap(),
            (
                "ab1c".to_string(),
                AllocationRequest::Compact(ResourceAmount::new_units(10))
            )
        );
        assert_eq!(
            parse_resource_request("a=5_000 compact").unwrap(),
            (
                "a".to_string(),
                AllocationRequest::Compact(ResourceAmount::new_units(5000))
            )
        );
        assert_eq!(
            parse_resource_request("cpus=351 scatter").unwrap(),
            (
                "cpus".to_string(),
                AllocationRequest::Scatter(ResourceAmount::new_units(351))
            )
        );
        assert_eq!(
            parse_resource_request("cpus=11 compact!").unwrap(),
            (
                "cpus".to_string(),
                AllocationRequest::ForceCompact(ResourceAmount::new_units(11))
            )
        );
    }

    #[test]
    fn test_parse_no_name() {
        assert_eq!(
            expect_parser_error(all_consuming(p_resource_request()), "=1"),
            "Unexpected token found, expected something else:\n  =1\n  |\n  --- Unexpected token `=`\n",
        );
    }

    #[test]
    fn test_parse_identifier_slash() {
        assert_eq!(
            parse_resource_request("a/b=1").unwrap(),
            (
                "a/b".to_string(),
                AllocationRequest::Compact(ResourceAmount::new_units(1))
            )
        );
    }

    #[test]
    fn test_parse_identifier_start_with_digit() {
        assert_eq!(
            expect_parser_error(all_consuming(p_resource_request()), "1a=1"),
            "Unexpected token found, expected something else:\n  1a=1\n  |\n  --- Unexpected token `1`\n",
        );
    }

    #[test]
    fn test_parse_zero_resources() {
        assert_eq!(
            expect_parser_error(all_consuming(p_resource_request()), "aa=0"),
            "Unexpected end of input found, expected something else:\n  aa=0\n     |\n     --- Error: Zero resources cannot be requested\n",
        );
    }

    #[test]
    fn test_parse_resource_request_error() {
        assert_eq!(
            expect_parser_error(all_consuming(p_resource_request()), ""),
            "Unexpected end of input found, expected something else:\n(the input was empty)",
        );
        assert_eq!(
            expect_parser_error(all_consuming(p_resource_request()), "a"),
            "Unexpected end of input found, expected =:\n  a\n   |\n   --- Unexpected end of input\n",
        );
        assert_eq!(
            expect_parser_error(all_consuming(p_resource_request()), "a=x"),
            "Unexpected token found, expected all:\n  a=x\n    |\n    --- Unexpected token `x`\n",
        );
    }
}
