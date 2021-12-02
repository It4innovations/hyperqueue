use nom::branch::alt;
use nom::character::complete::{alphanumeric1, char, multispace0, multispace1};
use nom::combinator::{map, map_res, opt};
use nom::sequence::{preceded, separated_pair, tuple};
use nom_supreme::tag::complete::tag;
use nom_supreme::ParserExt;

use tako::common::resources::{CpuRequest, GenericResourceAmount};

use crate::common::parser::{consume_all, p_u32, p_u64, NomResult};

fn p_cpu_request(input: &str) -> NomResult<CpuRequest> {
    alt((
        map(tag("all"), |_| CpuRequest::All),
        map_res(
            tuple((
                p_u32,
                opt(preceded(
                    multispace1,
                    alt((tag("compact!"), tag("compact"), tag("scatter"))),
                )),
            )),
            |(count, policy)| {
                if count == 0 {
                    return Err(anyhow::anyhow!("Requesting zero cpus is not allowed"));
                }
                Ok(match policy {
                    None | Some("compact") => CpuRequest::Compact(count),
                    Some("compact!") => CpuRequest::ForceCompact(count),
                    Some("scatter") => CpuRequest::Scatter(count),
                    _ => unreachable!(),
                })
            },
        ),
    ))(input)
}

pub fn parse_cpu_request(input: &str) -> anyhow::Result<CpuRequest> {
    consume_all(p_cpu_request, input)
}

fn p_resource_request(input: &str) -> NomResult<(String, GenericResourceAmount)> {
    map(
        separated_pair(
            alphanumeric1.context("Resource identifier"),
            tuple((multispace0, char('='), multispace0)),
            p_u64.context("Resource amount"),
        ),
        |(name, value)| (name.to_string(), value),
    )(input)
}

pub fn parse_resource_request(input: &str) -> anyhow::Result<(String, GenericResourceAmount)> {
    consume_all(p_resource_request, input)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::tests::utils::check_parse_error;

    #[test]
    fn test_parse_cpu_request() {
        assert_eq!(parse_cpu_request("all").unwrap(), CpuRequest::All);
        assert_eq!(parse_cpu_request("10").unwrap(), CpuRequest::Compact(10));
        assert_eq!(
            parse_cpu_request("5 compact").unwrap(),
            CpuRequest::Compact(5)
        );
        assert_eq!(
            parse_cpu_request("351 compact!").unwrap(),
            CpuRequest::ForceCompact(351)
        );
        assert_eq!(
            parse_cpu_request("10 scatter").unwrap(),
            CpuRequest::Scatter(10)
        );
    }

    #[test]
    fn test_parse_zero_cpus() {
        check_parse_error(
            p_cpu_request,
            "0",
            r#"Parse error
"Requesting zero cpus is not allowed" at character 0: "0""#,
        );
    }

    #[test]
    fn test_parse_resource_request() {
        assert_eq!(
            parse_resource_request("Abc=10_234").unwrap(),
            ("Abc".to_string(), 10234)
        );
        assert_eq!(
            parse_resource_request("X = 1").unwrap(),
            ("X".to_string(), 1)
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
            r#"Parse error
expected Resource amount at character 2: "x"
expected integer at character 2: "x""#,
        );
    }
}
