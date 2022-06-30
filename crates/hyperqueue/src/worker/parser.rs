use crate::arg_wrapper;
use crate::common::parser2::{all_consuming, parse_u32, parse_u64, CharParser};
use chumsky::error::Simple;
use chumsky::primitive::just;
use chumsky::text::{whitespace, TextParser};
use chumsky::Parser;
use tako::resources::{
    cpu_descriptor_from_socket_size, DescriptorError, GenericResourceDescriptorKind,
};
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

/// Parses a list resource.
/// The list must be non-empty and it has to contain uniaue values.
/// Example: `list(1, 2)`.
fn parse_resource_list() -> impl CharParser<GenericResourceDescriptorKind> {
    let start = just("list").then(just('(').padded());
    let end = just(')').padded();

    let indices = parse_u32()
        .separated_by(just(',').padded())
        .delimited_by(start, end)
        .labelled("list indices");
    indices
        .try_map(|indices, span| {
            if indices.is_empty() {
                Err(Simple::custom(
                    span,
                    "List has to contain at least a single element",
                ))
            } else {
                GenericResourceDescriptorKind::list(indices).map_err(|error| match error {
                    DescriptorError::GenericResourceListItemsNotUnique => {
                        Simple::custom(span, "List items have to be unique")
                    }
                })
            }
        })
        .labelled("list resource")
}

/// Parses a range resource.
/// The start of the range must be smaller or equal to the end.
/// Example: `range(1-5)`.
fn parse_resource_range() -> impl CharParser<GenericResourceDescriptorKind> {
    let start = just("range").then(just('(').padded());
    let end = just(')').padded();

    let range = parse_u32()
        .labelled("start")
        .then_ignore(just('-').padded())
        .then(parse_u32().labelled("end"))
        .labelled("range");

    range
        .delimited_by(start, end)
        .try_map(|(start, end), span| {
            if start > end {
                Err(Simple::custom(
                    span,
                    "Start must be greater or equal to end",
                ))
            } else {
                Ok(GenericResourceDescriptorKind::Range {
                    start: start.into(),
                    end: end.into(),
                })
            }
        })
        .labelled("range resource")
}

/// Parse a sum resource.
/// Example: `sum(100)`.
fn parse_resource_sum() -> impl CharParser<GenericResourceDescriptorKind> {
    let start = just("sum").then(just('(').padded());
    let end = just(')').padded();

    let value = parse_u64()
        .labelled("sum")
        .map(|size| GenericResourceDescriptorKind::Sum { size });

    value.delimited_by(start, end).labelled("sum resource")
}

/// Parses a resource definition, which consists of a name and a resource kind.
/// Example: `mem=list(1,2)`, `disk=sum(10)`, `foo=range(1-2)`.
fn parse_resource_definition_inner() -> impl CharParser<GenericResourceDescriptor> {
    let name = chumsky::text::ident()
        .repeated()
        .padded()
        .collect::<String>();
    let equal = just('=').padded_by(whitespace());
    let kind = (parse_resource_list()
        .or(parse_resource_range())
        .or(parse_resource_sum()))
    .labelled("Resource kind");

    name.then_ignore(equal)
        .then(kind)
        .map(|(name, kind)| GenericResourceDescriptor { name, kind })
}

fn parse_resource_definition(input: &str) -> anyhow::Result<GenericResourceDescriptor> {
    all_consuming(parse_resource_definition_inner()).parse_text(input)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::tests::utils::expect_parser_error;
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
        Unexpected end of input found, expected , or ]:
          [[1]
              |
              --- Unexpected end of input
        "###);
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
    fn test_parse_resource_def_list_whitespace() {
        let rd = parse_resource_definition("   mem    =    list    (   1  ) ").unwrap();
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
        insta::assert_snapshot!(expect_parser_error(parse_resource_definition_inner(), "mem=list(1,2,1)"), @r###"
        Unexpected end of input found while attempting to parse list resource, expected something else:
          mem=list(1,2,1)
              |
              --- List items have to be unique
        "###);
    }

    #[test]
    fn test_parse_resource_def_list_empty() {
        insta::assert_snapshot!(expect_parser_error(parse_resource_definition_inner(), "mem=list()"), @r###"
        Unexpected end of input found while attempting to parse list resource, expected something else:
          mem=list()
              |
              --- List has to contain at least a single element
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
    fn test_parse_resource_def_range_empty() {
        insta::assert_snapshot!(expect_parser_error(parse_resource_definition_inner(), "gpu=range()"), @r###"
        Unexpected token found while attempting to parse number, expected something else:
          gpu=range()
                    |
                    --- Unexpected token `)`
        "###);
    }

    #[test]
    fn test_parse_resource_def_missing_end() {
        insta::assert_snapshot!(expect_parser_error(parse_resource_definition_inner(), "gpu=range(10)"), @r###"
        Unexpected token found while attempting to parse range, expected - or _:
          gpu=range(10)
                      |
                      --- Unexpected token `)`
        "###);
    }

    #[test]
    fn test_parse_resource_def_start_larger_than_end() {
        insta::assert_snapshot!(expect_parser_error(parse_resource_definition_inner(), "gpu=range(5-3)"), @r###"
        Unexpected end of input found while attempting to parse range resource, expected something else:
          gpu=range(5-3)
              |
              --- Start must be greater or equal to end
        "###);
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
    fn test_parse_resource_def_sum_missing_value() {
        insta::assert_snapshot!(expect_parser_error(parse_resource_definition_inner(), "x=sum()"), @r###"
        Unexpected token found while attempting to parse number, expected something else:
          x=sum()
                |
                --- Unexpected token `)`
        "###);
    }

    #[test]
    fn test_parse_resource_def_sum_invalid_value() {
        insta::assert_snapshot!(expect_parser_error(parse_resource_definition_inner(), "x=sum(x)"), @r###"
        Unexpected token found while attempting to parse number, expected something else:
          x=sum(x)
                |
                --- Unexpected token `x`
        "###);
    }

    #[test]
    fn test_parse_resource_def_empty() {
        insta::assert_snapshot!(expect_parser_error(parse_resource_definition_inner(), ""), @r###"
        Unexpected end of input found while attempting to parse Resource kind, expected something else:
          gpu=range(5-3)
              |
              --- Start must be greater or equal to end
        "###);
    }

    #[test]
    fn test_parse_resource_def_number() {
        insta::assert_snapshot!(expect_parser_error(parse_resource_definition_inner(), "1"), @r###"
        Unexpected end of input found while attempting to parse Resource kind, expected something else:
          gpu=range(5-3)
              |
              --- Start must be greater or equal to end
        "###);
    }

    #[test]
    fn test_parse_resource_def_missing_value() {
        insta::assert_snapshot!(expect_parser_error(parse_resource_definition_inner(), "x="), @r###"
        Unexpected end of input found while attempting to parse Resource kind, expected something else:
          gpu=range(5-3)
              |
              --- Start must be greater or equal to end
        "###);
    }

    #[test]
    fn test_parse_resource_def_numeric_value() {
        insta::assert_snapshot!(expect_parser_error(parse_resource_definition_inner(), "x=1"), @r###"
        Unexpected end of input found while attempting to parse Resource kind, expected something else:
          gpu=range(5-3)
              |
              --- Start must be greater or equal to end
        "###);
    }

    #[test]
    fn test_parse_resource_def_only_sum() {
        insta::assert_snapshot!(expect_parser_error(parse_resource_definition_inner(), "x=sum"), @r###"
        Unexpected end of input found while attempting to parse sum resource, expected (:
          x=sum
               |
               --- Unexpected end of input
        "###);
    }
}
