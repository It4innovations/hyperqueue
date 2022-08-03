use nom::branch::alt;
use nom::character::complete::{alphanumeric1, char, multispace0, multispace1};
use nom::combinator::{map, map_res, opt};
use nom::sequence::{preceded, separated_pair, tuple};
use nom_supreme::tag::complete::tag;
use nom_supreme::ParserExt;

use tako::resources::{AllocationRequest, ResourceAmount};

use crate::common::parser::{consume_all, p_u64, NomResult};

fn p_allocation_request(input: &str) -> NomResult<AllocationRequest> {
    alt((
        map(tag("all"), |_| AllocationRequest::All),
        map_res(
            tuple((
                p_u64,
                opt(preceded(
                    multispace1,
                    alt((tag("compact!"), tag("compact"), tag("scatter"))),
                )),
            )),
            |(count, policy)| {
                let count = count as ResourceAmount;
                if count == 0 {
                    return Err(anyhow::anyhow!("Requesting zero resources is not allowed"));
                }
                Ok(match policy {
                    None | Some("compact") => AllocationRequest::Compact(count),
                    Some("compact!") => AllocationRequest::ForceCompact(count),
                    Some("scatter") => AllocationRequest::Scatter(count),
                    _ => unreachable!(),
                })
            },
        ),
    ))(input)
}

fn p_resource_request(input: &str) -> NomResult<(String, AllocationRequest)> {
    map(
        separated_pair(
            alphanumeric1.context("Resource identifier"),
            tuple((multispace0, char('='), multispace0)),
            p_allocation_request.context("Resource amount"),
        ),
        |(name, value)| (name.to_string(), value),
    )(input)
}

pub fn parse_resource_request(input: &str) -> anyhow::Result<(String, AllocationRequest)> {
    consume_all(p_resource_request, input)
}

pub fn parse_allocation_request(input: &str) -> anyhow::Result<AllocationRequest> {
    consume_all(p_allocation_request, input)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::tests::utils::check_parse_error;

    #[test]
    fn test_parse_resource_request() {
        assert_eq!(
            parse_resource_request("xxx=all").unwrap(),
            ("xxx".to_string(), AllocationRequest::All)
        );
        assert_eq!(
            parse_resource_request("ab1c=10").unwrap(),
            ("ab1c".to_string(), AllocationRequest::Compact(10))
        );
        assert_eq!(
            parse_resource_request("a=5_000 compact").unwrap(),
            ("a".to_string(), AllocationRequest::Compact(5000))
        );
        assert_eq!(
            parse_resource_request("cpus=351 scatter").unwrap(),
            ("cpus".to_string(), AllocationRequest::Scatter(351))
        );
        assert_eq!(
            parse_resource_request("cpus=11 compact!").unwrap(),
            ("cpus".to_string(), AllocationRequest::ForceCompact(11))
        );
    }

    #[test]
    fn test_parse_zero_resources() {
        check_parse_error(
            p_resource_request,
            "aa=0",
            r#"Parse error
expected Resource amount at character 3: "0"
  "Requesting zero resources is not allowed" at character 3: "0""#,
        );
    }

    #[test]
    fn test_parse_resource_request_error() {
        check_parse_error(
            p_resource_request,
            "",
            r#"Parse error
expected Resource identifier at the end of input
  expected alphanumeric character at the end of input"#,
        );
        check_parse_error(
            p_resource_request,
            "a",
            r#"Parse error
expected "=" at the end of input"#,
        );
        check_parse_error(
            p_resource_request,
            "a=x",
            "Parse error\nexpected Resource amount at character 2: \"x\"\n  expected one of the following 2 variants:\n    expected \"all\" at character 2: \"x\"\n    or\n    expected integer at character 2: \"x\"",
        );
    }
}
