use crate::arg_wrapper;
use crate::common::parser::{p_u32, p_u64, NomResult};
use anyhow::anyhow;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{alphanumeric1, char, multispace0};
use nom::combinator::{all_consuming, map, opt};
use nom::sequence::{delimited, preceded, separated_pair, tuple};
use tako::common::resources::descriptor::{
    cpu_descriptor_from_socket_size, GenericResourceDescriptorKind, GenericResourceKindIndices,
    GenericResourceKindSum,
};
use tako::common::resources::{CpusDescriptor, GenericResourceDescriptor};

fn p_cpu_definition(input: &str) -> NomResult<CpusDescriptor> {
    map(
        tuple((p_u32, opt(preceded(tag("x"), p_u32)))),
        |r| match r {
            (c1, None) => cpu_descriptor_from_socket_size(1, c1),
            (c1, Some(c2)) => cpu_descriptor_from_socket_size(c1, c2),
        },
    )(input)
}

fn parse_cpu_definition(input: &str) -> anyhow::Result<CpusDescriptor> {
    all_consuming(p_cpu_definition)(input)
        .map(|r| r.1)
        .map_err(|e| anyhow!(e.to_string()))
}

arg_wrapper!(ArgCpuDef, CpusDescriptor, parse_cpu_definition);
arg_wrapper!(
    ArgGenericResourceDef,
    GenericResourceDescriptor,
    parse_resource_definition
);

fn p_kind_indices(input: &str) -> NomResult<GenericResourceDescriptorKind> {
    map(
        delimited(
            tuple((tag("indices"), multispace0, char('('), multispace0)),
            separated_pair(p_u32, tuple((multispace0, char('-'), multispace0)), p_u32),
            tuple((multispace0, char(')'), multispace0)),
        ),
        |(start, end)| {
            GenericResourceDescriptorKind::Indices(GenericResourceKindIndices { start, end })
        },
    )(input)
}

fn p_kind_sum(input: &str) -> NomResult<GenericResourceDescriptorKind> {
    map(
        delimited(
            tuple((tag("sum"), multispace0, char('('), multispace0)),
            p_u64,
            tuple((multispace0, char(')'), multispace0)),
        ),
        |size| GenericResourceDescriptorKind::Sum(GenericResourceKindSum { size }),
    )(input)
}

fn p_resource_kind(input: &str) -> NomResult<GenericResourceDescriptorKind> {
    alt((p_kind_indices, p_kind_sum))(input)
}

fn p_resource_definition(input: &str) -> NomResult<GenericResourceDescriptor> {
    let parser = separated_pair(
        alphanumeric1,
        tuple((multispace0, char('='), multispace0)),
        p_resource_kind,
    );
    map(parser, |(name, kind)| GenericResourceDescriptor {
        name: name.to_string(),
        kind,
    })(input)
}

fn parse_resource_definition(input: &str) -> anyhow::Result<GenericResourceDescriptor> {
    all_consuming(p_resource_definition)(input)
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

    #[test]
    fn test_parse_resource_def() {
        let rd = parse_resource_definition("gpu=indices(10-123)").unwrap();
        assert_eq!(rd.name, "gpu");
        assert!(matches!(
            rd.kind,
            GenericResourceDescriptorKind::Indices(GenericResourceKindIndices {
                start: 10,
                end: 123
            })
        ));

        let rd = parse_resource_definition("mem=sum(1000_3000_2000)").unwrap();
        assert_eq!(rd.name, "mem");
        assert!(matches!(
            rd.kind,
            GenericResourceDescriptorKind::Sum(GenericResourceKindSum {
                size: 1000_3000_2000
            })
        ));
    }
}
