use crate::arg_wrapper;
use crate::common::parser2::{
    all_consuming, parse_exact_string, parse_named_string, parse_u32, parse_u64, CharParser,
    ParseError,
};
use chumsky::primitive::just;
use chumsky::text::TextParser;
use chumsky::Parser;
use tako::resources::{
    DescriptorError, ResourceAmount, ResourceDescriptorItem, ResourceDescriptorKind, ResourceIndex,
};

fn parse_cpu_definition(input: &str) -> anyhow::Result<ResourceDescriptorKind> {
    if let Ok(num) = input.parse::<u32>() {
        return Ok(ResourceDescriptorKind::simple_indices(num));
    }
    all_consuming(parse_resource_kind()).parse_text(input)
}

arg_wrapper!(
    ArgCpuDefinition,
    ResourceDescriptorKind,
    parse_cpu_definition
);
arg_wrapper!(
    ArgResourceItemDef,
    ResourceDescriptorItem,
    parse_resource_definition
);

fn parse_resource_indices() -> impl CharParser<Vec<ResourceIndex>> {
    let start = just('[').padded();
    let end = just(']').padded();

    parse_u32()
        .map(ResourceIndex::new)
        .separated_by(just(',').padded())
        .delimited_by(start, end)
}

/// Parsers a simple CPU descriptor like `1` or `2x4`.
/// When there's a single number, it states the number of CPUs.
/// When there are two numbers, the first is the number of sockets and the second the number of CPUs
/// per socket.
fn parse_resource_group_x_notation() -> impl CharParser<ResourceDescriptorKind> {
    parse_u32()
        .then(just('x').ignore_then(parse_u32()))
        .map(|(groups, group_size)| {
            ResourceDescriptorKind::regular_sockets(
                groups as ResourceAmount,
                group_size as ResourceAmount,
            )
        })
}

fn parse_resource_group() -> impl CharParser<ResourceDescriptorKind> {
    let start = just('[').padded();
    let end = just(']').padded();

    parse_resource_indices()
        .try_map(|group, span| {
            if group.is_empty() {
                Err(ParseError::custom(
                    span,
                    "Group has to contain at least a single element",
                ))
            } else {
                Ok(group)
            }
        })
        .separated_by(just(',').padded())
        .at_least(1)
        .delimited_by(start, end)
        .try_map(|groups, span| {
            ResourceDescriptorKind::groups(groups).map_err(|error| match error {
                DescriptorError::ResourceListItemsNotUnique => {
                    ParseError::custom(span, "Group items have to be unique")
                }
            })
        })
}

/// Parses a list resource.
/// The list must be non-empty and it has to contain uniaue values.
/// Example: `[1, 2]`.
fn parse_resource_list() -> impl CharParser<ResourceDescriptorKind> {
    parse_resource_indices().try_map(|indices, span| {
        if indices.is_empty() {
            Err(ParseError::custom(
                span,
                "List has to contain at least a single element",
            ))
        } else {
            ResourceDescriptorKind::list(indices).map_err(|error| match error {
                DescriptorError::ResourceListItemsNotUnique => {
                    ParseError::custom(span, "List items have to be unique")
                }
            })
        }
    })
}

/// Parses a range resource.
/// The start of the range must be smaller or equal to the end.
/// Example: `range(1-5)`.
fn parse_resource_range() -> impl CharParser<ResourceDescriptorKind> {
    let start = parse_exact_string("range").then(just('(').padded());
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
                Err(ParseError::custom(
                    span,
                    "Start must be greater or equal to end",
                ))
            } else {
                Ok(ResourceDescriptorKind::Range {
                    start: start.into(),
                    end: end.into(),
                })
            }
        })
}

/// Parse a sum resource.
/// Example: `sum(100)`.
fn parse_resource_sum() -> impl CharParser<ResourceDescriptorKind> {
    let start = parse_exact_string("sum").then(just('(').padded());
    let end = just(')').padded();

    let value = parse_u64()
        .labelled("sum")
        .map(|size| ResourceDescriptorKind::Sum { size });

    value.delimited_by(start, end)
}

fn parse_resource_kind() -> impl CharParser<ResourceDescriptorKind> {
    chumsky::primitive::choice((
        parse_resource_group_x_notation(),
        parse_resource_group(),
        parse_resource_list(),
        parse_resource_range(),
        parse_resource_sum(),
    ))
    .labelled("resource kind")
}

/// Parses a resource definition, which consists of a name and a resource kind.
/// Example: `mem=list(1,2)`, `disk=sum(10)`, `foo=range(1-2)`.
fn parse_resource_definition_inner() -> impl CharParser<ResourceDescriptorItem> {
    let name = parse_named_string("identifier")
        .padded()
        .labelled("resource name");
    let equal = just('=').padded();
    let kind = parse_resource_kind();

    name.then_ignore(equal)
        .then(kind)
        .map(|(name, kind)| ResourceDescriptorItem { name, kind })
        .labelled("resource definition")
}

fn parse_resource_definition(input: &str) -> anyhow::Result<ResourceDescriptorItem> {
    all_consuming(parse_resource_definition_inner()).parse_text(input)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::tests::utils::expect_parser_error;
    use tako::AsIdVec;

    #[test]
    fn test_parse_cpu_single_number() {
        let kind = parse_cpu_definition("4").unwrap();
        match kind {
            ResourceDescriptorKind::Range { start, end } => {
                assert_eq!(start.as_num(), 0);
                assert_eq!(end.as_num(), 3);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_parse_cpu_list_kind() {
        let kind = parse_cpu_definition("[0, 5, 7]").unwrap();
        match kind {
            ResourceDescriptorKind::List { values } => {
                assert_eq!(values, vec![0, 5, 7].to_ids());
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_parse_resource_group_x_notation() {
        let rd = parse_resource_definition("cpus=2x3").unwrap();
        assert_eq!(rd.name, "cpus");
        match rd.kind {
            ResourceDescriptorKind::Groups { groups } => {
                assert_eq!(groups, vec![vec![0, 1, 2].to_ids(), vec![3, 4, 5].to_ids()]);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_parse_resource_group_single() {
        let rd = parse_resource_definition("cpus=[[5, 7, 123]]").unwrap();
        assert_eq!(rd.name, "cpus");
        match rd.kind {
            ResourceDescriptorKind::List { values } => {
                assert_eq!(values, vec![5, 7, 123].to_ids());
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_parse_resource_group_multiple() {
        let rd = parse_resource_definition("cpus=[[0], [7], [123, 200]]").unwrap();
        assert_eq!(rd.name, "cpus");
        match rd.kind {
            ResourceDescriptorKind::Groups { groups } => {
                assert_eq!(
                    groups,
                    vec![vec![0].to_ids(), vec![7].to_ids(), vec![123, 200].to_ids()]
                );
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_parse_resource_group_unclosed_bracket() {
        insta::assert_snapshot!(expect_parser_error(parse_resource_definition_inner(), "xxx=[[1]"), @r###"
        Unexpected end of input found while attempting to parse resource kind, expected , or ]:
          xxx=[[1]
                  |
                  --- Unexpected end of input
        "###);
    }

    #[test]
    fn test_parse_resource_groups_with_empty_group() {
        insta::assert_snapshot!(expect_parser_error(parse_resource_definition_inner(), "cpus=[[0, 1], []]"), @r###"
        Unexpected end of input found while attempting to parse resource kind, expected something else:
          cpus=[[0, 1], []]
                        |
                        --- Group has to contain at least a single element
        "###);
    }

    #[test]
    fn test_parse_resource_def_list_single() {
        let rd = parse_resource_definition("mem=[1]").unwrap();
        assert_eq!(rd.name, "mem");
        match rd.kind {
            ResourceDescriptorKind::List { values } => {
                assert_eq!(values, vec![1.into()]);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_parse_resource_def_list_whitespace() {
        let rd = parse_resource_definition("   mem    =   [   1  ] ").unwrap();
        assert_eq!(rd.name, "mem");
        match rd.kind {
            ResourceDescriptorKind::List { values } => {
                assert_eq!(values, vec![1.into()]);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_parse_resource_def_list_multiple() {
        let rd = parse_resource_definition("mem=[12,34,58]").unwrap();
        assert_eq!(rd.name, "mem");
        match rd.kind {
            ResourceDescriptorKind::List { values } => {
                assert_eq!(values, vec![12.into(), 34.into(), 58.into()]);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_parse_resource_def_list_non_unique() {
        insta::assert_snapshot!(expect_parser_error(parse_resource_definition_inner(), "mem=[1,2,1]"), @r###"
        Unexpected end of input found while attempting to parse resource kind, expected something else:
          mem=[1,2,1]
              |
              --- List items have to be unique
        "###);
    }

    #[test]
    fn test_parse_resource_def_list_empty() {
        insta::assert_snapshot!(expect_parser_error(parse_resource_definition_inner(), "mem=[]"), @r###"
        Unexpected end of input found while attempting to parse resource kind, expected something else:
          mem=[]
              |
              --- List has to contain at least a single element
        "###);
    }

    #[test]
    fn test_parse_resource_def_range() {
        let rd = parse_resource_definition("gpu=range(10-123)").unwrap();
        assert_eq!(rd.name, "gpu");
        match rd.kind {
            ResourceDescriptorKind::Range { start, end } => {
                assert_eq!(start.as_num(), 10);
                assert_eq!(end.as_num(), 123);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_parse_resource_def_range_whitespace() {
        let rd = parse_resource_definition("  gpu  =  range  ( 10 -  123 ) ").unwrap();
        assert_eq!(rd.name, "gpu");
        match rd.kind {
            ResourceDescriptorKind::Range { start, end } => {
                assert_eq!(start.as_num(), 10);
                assert_eq!(end.as_num(), 123);
            }
            _ => panic!(),
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
        Unexpected end of input found while attempting to parse resource kind, expected something else:
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
            ResourceDescriptorKind::Sum {
                size: 1000_3000_2000
            }
        ));
    }

    #[test]
    fn test_parse_resource_def_sum_whitespace() {
        let rd = parse_resource_definition("   mem  = sum ( 1000_3000_2000 ) ").unwrap();
        assert_eq!(rd.name, "mem");
        assert!(matches!(
            rd.kind,
            ResourceDescriptorKind::Sum {
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
        Unexpected end of input found while attempting to parse resource name, expected identifier:
        (the input was empty)
        "###);
    }

    #[test]
    fn test_parse_resource_def_number() {
        insta::assert_snapshot!(expect_parser_error(parse_resource_definition_inner(), "1"), @r###"
        Unexpected token found while attempting to parse resource name, expected identifier:
          1
          |
          --- Unexpected token `1`
        "###);
    }

    #[test]
    fn test_parse_resource_def_missing_value() {
        insta::assert_snapshot!(expect_parser_error(parse_resource_definition_inner(), "x="), @r###"
        Unexpected end of input found while attempting to parse resource kind, expected [ or range or sum:
          x=
            |
            --- Unexpected end of input
        "###);
    }

    #[test]
    fn test_parse_resource_def_invalid_resource_kind() {
        insta::assert_snapshot!(expect_parser_error(parse_resource_definition_inner(), "x=foo"), @r###"
        Unexpected token found while attempting to parse resource kind, expected range or sum:
          x=foo
            |
            --- Unexpected token `foo`
        "###);
    }

    #[test]
    fn test_parse_resource_def_numeric_value() {
        insta::assert_snapshot!(expect_parser_error(parse_resource_definition_inner(), "x=1"), @r###"
        Unexpected end of input found while attempting to parse resource kind, expected _ or x:
          x=1
             |
             --- Unexpected end of input
        "###);
    }

    #[test]
    fn test_parse_resource_def_only_sum() {
        insta::assert_snapshot!(expect_parser_error(parse_resource_definition_inner(), "x=sum"), @r###"
        Unexpected end of input found while attempting to parse resource kind, expected ( or range:
          x=sum
               |
               --- Unexpected end of input
        "###);
    }
}
