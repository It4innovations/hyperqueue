use crate::arg_wrapper;
use crate::common::parser::{consume_all, p_u32, p_u64, NomResult};
use crate::common::parser2::{all_consuming, parse_u32, CharParser};
use chumsky::primitive::just;
use chumsky::text::whitespace;
use chumsky::Parser as Parser2;
use nom::branch::alt;
use nom::character::complete::{alphanumeric1, char, multispace0};
use nom::combinator::{map, map_res};
use nom::multi::separated_list1;
use nom::sequence::{delimited, separated_pair, tuple};
use nom::Parser;
use nom_supreme::tag::complete::tag;
use nom_supreme::ParserExt;
use tako::resources::{cpu_descriptor_from_socket_size, GenericResourceDescriptorKind};
use tako::resources::{CpuId, CpusDescriptor, GenericResourceDescriptor};

/// Parsers a simple CPU descriptor like `1` or `2x4`.
/// When there's a single number, it states the number of CPUs.
/// When there are two numbers, the first is the number of sockets and the second the number of CPUs
/// per socket.
fn parse_simple_cpu_descriptor() -> impl CharParser<CpusDescriptor> {
    let first = parse_u32();
    let second = just('x').ignore_then(parse_u32()).or_not();
    first.then(second).map(|parsed| match parsed {
        (cpus, None) => cpu_descriptor_from_socket_size(1, cpus),
        (sockets, Some(cpus)) => cpu_descriptor_from_socket_size(sockets, cpus),
    })
}

/// Parses a socket list where each item contains a list of CPUs.
/// For example: `[[0]]`, `[[1, 2], [2, 0]]`.
fn parse_socket_list_descriptor() -> impl CharParser<CpusDescriptor> {
    let start = just('[').then(whitespace());
    let end = whitespace().then(just(']'));
    let cpu_list = parse_u32()
        .map(CpuId::new)
        .separated_by(just(',').then(whitespace()))
        .delimited_by(start, end);

    cpu_list
        .separated_by(just(',').then(whitespace()))
        .delimited_by(start, end)
}

fn parse_cpu_definition_inner() -> impl CharParser<CpusDescriptor> {
    parse_simple_cpu_descriptor().or(parse_socket_list_descriptor())
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
        _ => all_consuming(parse_cpu_definition_inner())
            .parse_text(input)
            .map(CpuDefinition::Custom),
    }
}

arg_wrapper!(ArgCpuDefinition, CpuDefinition, parse_cpu_definition);
arg_wrapper!(
    ArgGenericResourceDef,
    GenericResourceDescriptor,
    parse_resource_definition
);

fn p_kind_list(input: &str) -> NomResult<GenericResourceDescriptorKind> {
    map_res(
        delimited(
            tuple((tag("list"), multispace0, char('('), multispace0)),
            separated_list1(tag(","), p_u32).context("At least a single index has to be provided"),
            tuple((multispace0, char(')'), multispace0)),
        ),
        |values| {
            GenericResourceDescriptorKind::list(values.into_iter().map(|idx| idx.into()).collect())
        },
    )(input)
}

fn p_kind_range(input: &str) -> NomResult<GenericResourceDescriptorKind> {
    map(
        delimited(
            tuple((tag("range"), multispace0, char('('), multispace0)),
            separated_pair(p_u32, tuple((multispace0, char('-'), multispace0)), p_u32),
            tuple((multispace0, char(')'), multispace0)),
        ),
        |(start, end)| GenericResourceDescriptorKind::Range {
            start: start.into(),
            end: end.into(),
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
        |size| GenericResourceDescriptorKind::Sum { size },
    )
    .parse(input)
}

fn p_resource_kind(input: &str) -> NomResult<GenericResourceDescriptorKind> {
    alt((
        p_kind_list.context("List resource"),
        p_kind_range.context("Range resource"),
        p_kind_sum.context("Sum resource"),
    ))(input)
}

pub fn p_resource_definition(input: &str) -> NomResult<GenericResourceDescriptor> {
    let parser = separated_pair(
        alphanumeric1.context("Resource identifier"),
        tuple((multispace0, char('='), multispace0)),
        p_resource_kind.context("Resource kind (sum, range or list)"),
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
    use crate::tests::utils::{check_parse_error, expect_parser_error};
    use tako::AsIdVec;

    #[test]
    fn test_parse_simple_cpu_def_single_cpu() {
        assert_eq!(
            parse_cpu_definition("4").unwrap(),
            CpuDefinition::Custom(vec![vec![0, 1, 2, 3].to_ids()]),
        );
    }

    #[test]
    fn test_parse_simple_cpu_def_multiple_sockets() {
        assert_eq!(
            parse_cpu_definition("2x3").unwrap(),
            CpuDefinition::Custom(vec![vec![0, 1, 2].to_ids(), vec![3, 4, 5].to_ids()]),
        );
    }

    #[test]
    fn test_parse_socket_list_single_socket() {
        assert_eq!(
            parse_cpu_definition("[[5, 7, 123]]").unwrap(),
            CpuDefinition::Custom(vec![vec![5, 7, 123].to_ids()]),
        );
    }

    #[test]
    fn test_parse_socket_multiple_sockets() {
        assert_eq!(
            parse_cpu_definition("[[0], [7], [123, 200]]").unwrap(),
            CpuDefinition::Custom(vec![
                vec![0].to_ids(),
                vec![7].to_ids(),
                vec![123, 200].to_ids()
            ]),
        );
    }

    #[test]
    fn test_parse_cpu_def_no_hyperthreading() {
        assert_eq!(
            parse_cpu_definition("no-ht").unwrap(),
            CpuDefinition::DetectNoHyperThreading,
        );
    }

    #[test]
    fn test_parse_cpu_def_auto() {
        assert_eq!(parse_cpu_definition("auto").unwrap(), CpuDefinition::Detect);
    }

    #[test]
    fn test_parse_cpu_def_invalid() {
        insta::assert_snapshot!(expect_parser_error(parse_cpu_definition_inner(), "x"), @r###"
        Unexpected token found while attempting to parse number, expected [:
          x
          |
          --- Unexpected token `x`
        "###);
    }

    #[test]
    fn test_parse_cpu_def_unclosed_bracket() {
        insta::assert_snapshot!(expect_parser_error(parse_cpu_definition_inner(), "[[1]"), @r###"
        Unexpected end of input found, expected ] or ,:
          [[1]
              |
              --- Unexpected end of input
        "###);
    }

    #[test]
    fn test_parse_resource_def_range() {
        let rd = parse_resource_definition("gpu=range(10-123)").unwrap();
        assert_eq!(rd.name, "gpu");
        match rd.kind {
            GenericResourceDescriptorKind::Range { start, end } => {
                assert_eq!(start.as_num(), 10);
                assert_eq!(end.as_num(), 123);
            }
            _ => panic!("Wrong result"),
        }
    }

    #[test]
    fn test_parse_resource_def_sum() {
        let rd = parse_resource_definition("mem=sum(1000_3000_2000)").unwrap();
        assert_eq!(rd.name, "mem");
        assert!(matches!(
            rd.kind,
            GenericResourceDescriptorKind::Sum {
                size: 1000_3000_2000
            }
        ));
    }

    #[test]
    fn test_parse_resource_def_list_single() {
        let rd = parse_resource_definition("mem=list(1)").unwrap();
        assert_eq!(rd.name, "mem");
        match rd.kind {
            GenericResourceDescriptorKind::List { values } => {
                assert_eq!(values, vec![1.into()]);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_parse_resource_def_list_multiple() {
        let rd = parse_resource_definition("mem=list(12,34,58)").unwrap();
        assert_eq!(rd.name, "mem");
        match rd.kind {
            GenericResourceDescriptorKind::List { values } => {
                assert_eq!(values, vec![12.into(), 34.into(), 58.into()]);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_parse_resource_def_list_non_unique() {
        check_parse_error(
            p_resource_definition,
            "mem=list(1,2,1)",
            r#"Parse error
expected Resource kind (sum, range or list) at character 4: "list(1,2,1)"
  expected one of the following 3 variants:
    expected List resource at character 4: "list(1,2,1)"
      "Items in a list-based generic resource have to be unique" at character 4: "list(1,2,1)"
    or
    expected Range resource at character 4: "list(1,2,1)"
      expected "range" at character 4: "list(1,2,1)"
    or
    expected Sum resource at character 4: "list(1,2,1)"
      expected "sum" at character 4: "list(1,2,1)""#,
        );
    }

    #[test]
    fn test_parse_resource_def_list_empty() {
        check_parse_error(
            p_resource_definition,
            "mem=list()",
            r#"Parse error
expected Resource kind (sum, range or list) at character 4: "list()"
  expected one of the following 3 variants:
    expected List resource at character 4: "list()"
    expected At least a single index has to be provided at character 9: ")"
    expected integer at character 9: ")"
    or
    expected Range resource at character 4: "list()"
      expected "range" at character 4: "list()"
    or
    expected Sum resource at character 4: "list()"
      expected "sum" at character 4: "list()""#,
        );
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
expected Resource kind (sum, range or list) at the end of input
  expected one of the following 3 variants:
    expected List resource at the end of input
      expected "list" at the end of input
    or
    expected Range resource at the end of input
      expected "range" at the end of input
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
expected Resource kind (sum, range or list) at character 2: "1"
  expected one of the following 3 variants:
    expected List resource at character 2: "1"
      expected "list" at character 2: "1"
    or
    expected Range resource at character 2: "1"
      expected "range" at character 2: "1"
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
expected Resource kind (sum, range or list) at character 2: "sum"
  expected one of the following 3 variants:
    expected List resource at character 2: "sum"
      expected "list" at character 2: "sum"
    or
    expected Range resource at character 2: "sum"
      expected "range" at character 2: "sum"
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
expected Resource kind (sum, range or list) at character 2: "sum()"
  expected one of the following 3 variants:
    expected List resource at character 2: "sum()"
      expected "list" at character 2: "sum()"
    or
    expected Range resource at character 2: "sum()"
      expected "range" at character 2: "sum()"
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
expected Resource kind (sum, range or list) at character 2: "sum(x)"
  expected one of the following 3 variants:
    expected List resource at character 2: "sum(x)"
      expected "list" at character 2: "sum(x)"
    or
    expected Range resource at character 2: "sum(x)"
      expected "range" at character 2: "sum(x)"
    or
    expected Sum resource at character 2: "sum(x)"
    expected integer at character 6: "x)""#,
        );
    }
}
