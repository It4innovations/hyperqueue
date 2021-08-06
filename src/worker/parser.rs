use crate::common::parser::{p_u32, NomResult};
use anyhow::anyhow;
use nom::bytes::complete::tag;
use nom::combinator::{all_consuming, map, opt};
use nom::sequence::{preceded, tuple};
use tako::common::resources::ResourceDescriptor;

fn p_cpu_definition(input: &str) -> NomResult<ResourceDescriptor> {
    map(
        tuple((p_u32, opt(preceded(tag("x"), p_u32)))),
        |r| match r {
            (c1, None) => ResourceDescriptor::new_with_socket_size(1, c1),
            (c1, Some(c2)) => ResourceDescriptor::new_with_socket_size(c1, c2),
        },
    )(input)
}

pub fn parse_cpu_definition(input: &str) -> anyhow::Result<ResourceDescriptor> {
    all_consuming(p_cpu_definition)(input)
        .map(|r| r.1)
        .map_err(|e| anyhow!(e.to_string()))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_cpu_def() {
        assert_eq!(
            parse_cpu_definition("4").unwrap().cpus,
            vec![vec![0, 1, 2, 3]]
        );
        assert_eq!(
            parse_cpu_definition("2x3").unwrap().cpus,
            vec![vec![0, 1, 2], vec![3, 4, 5]]
        );
    }
}
