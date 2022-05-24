use crate::arg_wrapper;
use crate::common::parser::{consume_all, p_u32, p_u64, NomResult};
use nom::branch::alt;
use nom::character::complete::{alphanumeric1, char, multispace0, space0};
use nom::combinator::{map, opt};
use nom::multi::separated_list1;
use nom::sequence::{delimited, preceded, separated_pair, tuple};
use nom::Parser;
use nom_supreme::tag::complete::tag;
use nom_supreme::ParserExt;
use tako::resources::{
    cpu_descriptor_from_socket_size, GenericResourceDescriptorKind, GenericResourceKindIndices,
    GenericResourceKindSum,
};
use tako::resources::{CpuId, CpusDescriptor, GenericResourceDescriptor};

fn p_cpu_list(input: &str) -> NomResult<Vec<CpuId>> {
    delimited(
        tuple((char('['), space0)),
        separated_list1(tuple((char(','), space0)), map(p_u32, |x| x.into())),
        tuple((space0, char(']'))),
    )(input)
}

fn p_cpu_socket_list(input: &str) -> NomResult<CpusDescriptor> {
    delimited(
        tuple((char('['), space0)),
        separated_list1(tuple((char(','), space0)), p_cpu_list),
        tuple((space0, char(']'))),
    )(input)
}

fn p_cpu_simple(input: &str) -> NomResult<CpusDescriptor> {
    map(
        tuple((p_u32, opt(preceded(char('x'), p_u32)))),
        |r| match r {
            (c1, None) => cpu_descriptor_from_socket_size(1, c1),
            (c1, Some(c2)) => cpu_descriptor_from_socket_size(c1, c2),
        },
    )(input)
}

fn p_cpu_definition(input: &str) -> NomResult<CpusDescriptor> {
    alt((p_cpu_simple, p_cpu_socket_list))(input)
}

#[derive(Debug)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub enum CpuDefinition {
    Detect,
    DetectNoHyperThreading,
    Custom(CpusDescriptor),
}

fn parse_cpu_definition(input: &str) -> anyhow::Result<CpuDefinition> {
    match input {
        "auto" => Ok(CpuDefinition::Detect),
        "no-ht" => Ok(CpuDefinition::DetectNoHyperThreading),
        other => consume_all(p_cpu_definition, other).map(CpuDefinition::Custom),
    }
}

arg_wrapper!(ArgCpuDefinition, CpuDefinition, parse_cpu_definition);
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
            GenericResourceDescriptorKind::Indices(GenericResourceKindIndices {
                start: start.into(),
                end: end.into(),
            })
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
    )
    .parse(input)
}

fn p_resource_kind(input: &str) -> NomResult<GenericResourceDescriptorKind> {
    alt((
        p_kind_indices.context("Index resource"),
        p_kind_sum.context("Sum resource"),
    ))(input)
}

pub fn p_resource_definition(input: &str) -> NomResult<GenericResourceDescriptor> {
    let parser = separated_pair(
        alphanumeric1.context("Resource identifier"),
        tuple((multispace0, char('='), multispace0)),
        p_resource_kind.context("Resource kind (sum or indices)"),
    );
    map(parser, |(name, kind)| GenericResourceDescriptor {
        name: name.to_string(),
        kind,
    })(input)
}

fn parse_resource_definition(input: &str) -> anyhow::Result<GenericResourceDescriptor> {
    consume_all(p_resource_definition, input)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::tests::utils::check_parse_error;
    use tako::AsIdVec;

    #[test]
    fn test_parse_cpu_def() {
        assert_eq!(
            parse_cpu_definition("4").unwrap(),
            CpuDefinition::Custom(vec![vec![0, 1, 2, 3].to_ids()]),
        );
        assert_eq!(
            parse_cpu_definition("2x3").unwrap(),
            CpuDefinition::Custom(vec![vec![0, 1, 2].to_ids(), vec![3, 4, 5].to_ids()]),
        );
        assert_eq!(
            parse_cpu_definition("[[5, 7, 123]]").unwrap(),
            CpuDefinition::Custom(vec![vec![5, 7, 123].to_ids()]),
        );
        assert_eq!(
            parse_cpu_definition("[[0], [7], [123, 200]]").unwrap(),
            CpuDefinition::Custom(vec![
                vec![0].to_ids(),
                vec![7].to_ids(),
                vec![123, 200].to_ids()
            ]),
        );
        assert_eq!(
            parse_cpu_definition("no-ht").unwrap(),
            CpuDefinition::DetectNoHyperThreading,
        );
        assert_eq!(parse_cpu_definition("auto").unwrap(), CpuDefinition::Detect);
    }

    #[test]
    fn test_parse_cpu_def_error() {
        check_parse_error(
            p_cpu_definition,
            "x",
            "Parse error\nexpected one of the following 2 variants:\n  expected integer at character 0: \"x\"\n  or\n  expected \"[\" at character 0: \"x\"",
        );
    }

    #[test]
    fn test_parse_resource_def() {
        let rd = parse_resource_definition("gpu=indices(10-123)").unwrap();
        assert_eq!(rd.name, "gpu");
        match rd.kind {
            GenericResourceDescriptorKind::Indices(indices) => {
                assert_eq!(indices.start.as_num(), 10);
                assert_eq!(indices.end.as_num(), 123);
            }
            _ => panic!("Wrong result"),
        }

        let rd = parse_resource_definition("mem=sum(1000_3000_2000)").unwrap();
        assert_eq!(rd.name, "mem");
        assert!(matches!(
            rd.kind,
            GenericResourceDescriptorKind::Sum(GenericResourceKindSum {
                size: 1000_3000_2000
            })
        ));
    }

    #[test]
    fn test_parse_resource_def_empty() {
        check_parse_error(
            p_resource_definition,
            "",
            r#"Parse error
expected Resource identifier at the end of input
  expected alphanumeric character at the end of input"#,
        );
    }

    #[test]
    fn test_parse_resource_def_number() {
        check_parse_error(
            p_resource_definition,
            "1",
            r#"Parse error
expected "=" at the end of input"#,
        );
    }

    #[test]
    fn test_parse_resource_def_missing_value() {
        check_parse_error(
            p_resource_definition,
            "x=",
            r#"Parse error
expected Resource kind (sum or indices) at the end of input
  expected one of the following 2 variants:
    expected Index resource at the end of input
      expected "indices" at the end of input
    or
    expected Sum resource at the end of input
      expected "sum" at the end of input"#,
        );
    }

    #[test]
    fn test_parse_resource_def_numeric_value() {
        check_parse_error(
            p_resource_definition,
            "x=1",
            r#"Parse error
expected Resource kind (sum or indices) at character 2: "1"
  expected one of the following 2 variants:
    expected Index resource at character 2: "1"
      expected "indices" at character 2: "1"
    or
    expected Sum resource at character 2: "1"
      expected "sum" at character 2: "1""#,
        );
    }

    #[test]
    fn test_parse_resource_def_only_sum() {
        check_parse_error(
            p_resource_definition,
            "x=sum",
            r#"Parse error
expected Resource kind (sum or indices) at character 2: "sum"
  expected one of the following 2 variants:
    expected Index resource at character 2: "sum"
      expected "indices" at character 2: "sum"
    or
    expected Sum resource at character 2: "sum"
      expected "(" at the end of input"#,
        );
    }

    #[test]
    fn test_parse_resource_def_missing_value_in_parentheses() {
        check_parse_error(
            p_resource_definition,
            "x=sum()",
            r#"Parse error
expected Resource kind (sum or indices) at character 2: "sum()"
  expected one of the following 2 variants:
    expected Index resource at character 2: "sum()"
      expected "indices" at character 2: "sum()"
    or
    expected Sum resource at character 2: "sum()"
    expected integer at character 6: ")""#,
        );
    }

    #[test]
    fn test_parse_resource_def_invalid_value_in_parentheses() {
        check_parse_error(
            p_resource_definition,
            "x=sum(x)",
            r#"Parse error
expected Resource kind (sum or indices) at character 2: "sum(x)"
  expected one of the following 2 variants:
    expected Index resource at character 2: "sum(x)"
      expected "indices" at character 2: "sum(x)"
    or
    expected Sum resource at character 2: "sum(x)"
    expected integer at character 6: "x)""#,
        );
    }
}
