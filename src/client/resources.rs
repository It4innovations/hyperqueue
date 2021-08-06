use crate::common::parser::{format_parse_error, p_u32, NomResult};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::multispace1;
use nom::combinator::{all_consuming, map, map_res, opt};
use nom::sequence::{preceded, tuple};
use tako::common::resources::CpuRequest;

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
    all_consuming(p_cpu_request)(input)
        .map(|r| r.1)
        .map_err(format_parse_error)
}

pub fn cpu_request_to_string(cr: &CpuRequest) -> String {
    match cr {
        CpuRequest::Compact(n_cpus) => {
            format!("{} compact", *n_cpus)
        }
        CpuRequest::ForceCompact(n_cpus) => {
            format!("{} compact!", *n_cpus)
        }
        CpuRequest::Scatter(n_cpus) => {
            format!("{} scatter", *n_cpus)
        }
        CpuRequest::All => "all".to_string(),
    }
}

#[cfg(test)]
mod test {
    use super::*;

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
        assert!(parse_cpu_request("0").is_err());
    }
}
