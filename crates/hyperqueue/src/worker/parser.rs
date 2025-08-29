use chumsky::primitive::{filter, just};
use chumsky::text::TextParser;
use chumsky::{Error, Parser};
use tako::resources::{
    DescriptorError, ResourceDescriptorCoupling, ResourceDescriptorCouplingItem,
    ResourceDescriptorItem, ResourceDescriptorKind, ResourceGroupIdx, ResourceUnits,
};

use crate::common::parser2::{
    CharParser, ParseError, all_consuming, parse_exact_string, parse_resource_amount, parse_u32,
};

pub fn parse_cpu_definition(input: &str) -> anyhow::Result<ResourceDescriptorKind> {
    if let Ok(num) = input.parse::<u32>() {
        return Ok(ResourceDescriptorKind::simple_indices(num));
    }
    all_consuming(parse_resource_kind()).parse_text(input)
}

/// Parses an individual resource label.
/// The label has to be wrapped in quotes if it needs to contain `[`, `]`, `,` or whitespace.
/// The label cannot contain quotes.
fn parse_individual_resource() -> impl CharParser<String> {
    let any_but_quote = chumsky::primitive::filter(|&c| c != '"')
        .repeated()
        .at_least(1);
    let bare_string = chumsky::primitive::filter(|&c: &char| match c {
        ',' | '[' | ']' | '"' => false,
        _ => !c.is_whitespace(),
    })
    .repeated()
    .at_least(1);

    let quoted = any_but_quote.delimited_by(just('"'), just('"'));
    quoted.or(bare_string).padded().collect()
}

fn parse_individual_resources() -> impl CharParser<Vec<String>> {
    let start = just('[').padded();
    let end = just(']').padded();

    parse_individual_resource()
        .separated_by(just(',').padded())
        .delimited_by(start, end)
}

/// Parses a simple CPU descriptor like `1` or `2x4`.
/// When there's a single number, it states the number of CPUs.
/// When there are two numbers, the first is the number of sockets and the second the number of CPUs
/// per socket.
fn parse_resource_group_x_notation() -> impl CharParser<ResourceDescriptorKind> {
    parse_u32()
        .then(just('x').ignore_then(parse_u32()))
        .map(|(groups, group_size)| {
            ResourceDescriptorKind::regular_sockets(
                groups as ResourceUnits,
                group_size as ResourceUnits,
            )
        })
}

fn parse_resource_group() -> impl CharParser<ResourceDescriptorKind> {
    let start = just('[').padded();
    let end = just(']').padded();

    parse_individual_resources()
        .try_map(|group, span| {
            if group.is_empty() {
                Err(ParseError::custom(
                    span,
                    "Each group has to contain at least a single element",
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
                DescriptorError::EmptyGroups => {
                    ParseError::custom(span, "There has to be at least a single group")
                }
            })
        })
}

/// Parses a list resource.
/// The list must be non-empty and it has to contain uniaue values.
/// The values inside the list can be either numbers or strings.
/// Strings can be put into quotes if you need to include a comma inside of them.
/// Example: `[1, 2]`, `[abc, def]`, `["abc","a,b",c]`.
fn parse_resource_list() -> impl CharParser<ResourceDescriptorKind> {
    parse_individual_resources().try_map(|indices, span| {
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
                error => ParseError::custom(span, error.to_string()),
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

    let value = parse_resource_amount()
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

pub fn is_valid_starting_resource_char(c: char) -> bool {
    c.is_ascii_alphabetic()
}

pub fn is_valid_resource_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '/'
}

fn parse_resource_name() -> impl CharParser<String> {
    let identifier = filter(|&c| is_valid_starting_resource_char(c))
        .map(Some)
        .chain::<char, Vec<char>, _>(filter(|&c| is_valid_resource_char(c)).repeated())
        .collect::<String>();

    identifier.map_err_with_span(|error: ParseError, span| {
        error.merge(ParseError::expected_input_found_string(
            span,
            Some(Some("identifier".to_string())),
            None,
        ))
    })
}

/// Parses a resource definition, which consists of a name and a resource kind.
/// Example: `mem=list(1,2)`, `disk=sum(10)`, `foo=range(1-2)`.
fn parse_resource_definition_inner() -> impl CharParser<ResourceDescriptorItem> {
    let name = parse_resource_name().padded().labelled("resource name");
    let equal = just('=').padded();
    let kind = parse_resource_kind();

    name.then_ignore(equal)
        .then(kind)
        .map(|(name, kind)| ResourceDescriptorItem { name, kind })
        .labelled("resource definition")
}

pub fn parse_resource_definition(input: &str) -> anyhow::Result<ResourceDescriptorItem> {
    all_consuming(parse_resource_definition_inner()).parse_text(input)
}

fn parse_resource_coupling_group(
    input: &str,
    resources: &[ResourceDescriptorItem],
) -> anyhow::Result<(u8, ResourceGroupIdx)> {
    let input = input.trim();
    let Some(input) = input.strip_suffix(']') else {
        return Err(anyhow::anyhow!("Missing ']'"));
    };
    let Some((left, right)) = input.split_once('[') else {
        return Err(anyhow::anyhow!("Missing '['"));
    };
    let Some(resource_idx) = resources.iter().position(|r| r.name == left) else {
        return Err(anyhow::anyhow!("Resource not found: {left}"));
    };
    let Ok(group_idx) = right.parse::<u8>() else {
        return Err(anyhow::anyhow!("Invalid group index: {right}"));
    };
    Ok((resource_idx as u8, group_idx.into()))
}

pub fn parse_resource_coupling(
    input: &str,
    resources: &[ResourceDescriptorItem],
) -> anyhow::Result<ResourceDescriptorCoupling> {
    let mut weights = input
        .split(",")
        .map(|s| {
            let s = s.trim();
            let (rs, weight) = (if let Some((rs, ws)) = s.split_once('=') {
                let ws = ws.trim();
                let Ok(w) = ws.parse() else {
                    return Err(anyhow::anyhow!("Invalid weight: {ws}"));
                };
                anyhow::Ok((rs, w))
            } else {
                anyhow::Ok((s, 256))
            })?;
            let Some((left, right)) = rs.split_once(':') else {
                return Err(anyhow::anyhow!("Missing ':'"));
            };
            let (resource1_idx, group1_idx) = parse_resource_coupling_group(left, resources)?;
            let (resource2_idx, group2_idx) = parse_resource_coupling_group(right, resources)?;
            let mut item = ResourceDescriptorCouplingItem {
                resource1_idx,
                group1_idx,
                resource2_idx,
                group2_idx,
                weight,
            };
            item.normalize();
            Ok(item)
        })
        .collect::<Result<Vec<_>, _>>()?;
    weights.sort_unstable();
    Ok(ResourceDescriptorCoupling { weights })
}

#[cfg(test)]
mod test {
    use crate::tests::utils::expect_parser_error;
    use tako::internal::tests::utils::shared::{
        res_kind_groups, res_kind_list, res_kind_range, res_kind_sum,
    };
    use tako::resources::ResourceAmount;

    use super::*;

    #[test]
    fn test_resource_name_empty() {
        insta::assert_snapshot!(expect_parser_error(parse_resource_name(), ""), @r###"
        Unexpected end of input found, expected identifier:
        (the input was empty)
        "###);
    }

    #[test]
    fn test_resource_name_normal() {
        assert_eq!(parse_resource_name().parse_text("asd").unwrap(), "asd");
    }

    #[test]
    fn test_resource_name_underscore() {
        assert_eq!(parse_resource_name().parse_text("a_b").unwrap(), "a");
    }

    #[test]
    fn test_resource_name_slash() {
        assert_eq!(
            parse_resource_name().parse_text("foo/bar").unwrap(),
            "foo/bar"
        );
    }

    #[test]
    fn test_resource_start_with_digit() {
        insta::assert_snapshot!(expect_parser_error(parse_resource_name(), "1a"), @r###"
        Unexpected token found, expected identifier:
          1a
          |
          --- Unexpected token `1`
        "###);
    }

    #[test]
    fn test_individual_resource_bare() {
        assert_eq!(
            parse_individual_resource().parse_text("asd").unwrap(),
            "asd"
        );
    }

    #[test]
    fn test_individual_resource_bare_comma() {
        assert_eq!(
            parse_individual_resource().parse_text("as,d").unwrap(),
            "as"
        );
    }

    #[test]
    fn test_individual_resource_bare_bracket() {
        assert_eq!(
            parse_individual_resource().parse_text("as]d").unwrap(),
            "as"
        );
    }

    #[test]
    fn test_individual_resource_bare_whitespace() {
        assert_eq!(
            parse_individual_resource().parse_text("  asd ").unwrap(),
            "asd"
        );
    }

    #[test]
    fn test_individual_resource_quoted() {
        assert_eq!(
            parse_individual_resource().parse_text(r#""asd""#).unwrap(),
            "asd"
        );
    }

    #[test]
    fn test_individual_resource_quoted_comma_bracket() {
        assert_eq!(
            parse_individual_resource()
                .parse_text(r#""asd,dsa,[x]""#)
                .unwrap(),
            "asd,dsa,[x]"
        );
    }

    #[test]
    fn test_individual_resource_quoted_whitespace_inside() {
        assert_eq!(
            parse_individual_resource()
                .parse_text(r#"" foo    bar""#)
                .unwrap(),
            " foo    bar"
        );
    }

    #[test]
    fn test_individual_resource_quoted_whitespace_outside() {
        assert_eq!(
            parse_individual_resource()
                .parse_text(r#"   " foo    bar"  "#)
                .unwrap(),
            " foo    bar"
        );
    }

    #[test]
    fn test_parse_cpu_single_number() {
        check_kind(parse_cpu_definition("4"), res_kind_range(0, 3));
    }

    #[test]
    fn test_parse_cpu_list_kind() {
        check_kind(
            parse_cpu_definition("[0, 5, 7]"),
            res_kind_list(&["0", "5", "7"]),
        );
    }

    #[test]
    fn test_parse_resource_group_x_notation() {
        check_item(
            parse_resource_definition("cpus=2x3"),
            "cpus",
            res_kind_groups(&[vec!["0", "1", "2"], vec!["3", "4", "5"]]),
        );
    }

    #[test]
    fn test_parse_resource_group_single() {
        check_item(
            parse_resource_definition("cpus=[[5, 7, 123]]"),
            "cpus",
            res_kind_list(&["5", "7", "123"]),
        );
    }

    #[test]
    fn test_parse_resource_group_multiple() {
        check_item(
            parse_resource_definition("cpus=[[0],[7],[123, 200]]"),
            "cpus",
            res_kind_groups(&[vec!["0"], vec!["7"], vec!["123", "200"]]),
        );
    }

    #[test]
    fn test_parse_resource_group_multiple_whitespace() {
        check_item(
            parse_resource_definition("cpus=[   [ 0 ] ,  [ 7  ] , [ 123  ,  200 ]  ]"),
            "cpus",
            res_kind_groups(&[vec!["0"], vec!["7"], vec!["123", "200"]]),
        );
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
                        --- Each group has to contain at least a single element
        "###);
    }

    #[test]
    fn test_parse_resource_def_list_single() {
        check_item(
            parse_resource_definition("mem=[1]"),
            "mem",
            res_kind_list(&["1"]),
        );
    }

    #[test]
    fn test_parse_resource_def_list_whitespace() {
        check_item(
            parse_resource_definition("   mem    =   [   1  ] "),
            "mem",
            res_kind_list(&["1"]),
        );
    }

    #[test]
    fn test_parse_resource_def_list_multiple() {
        check_item(
            parse_resource_definition("mem=[12,34,58]"),
            "mem",
            res_kind_list(&["12", "34", "58"]),
        );
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
        check_item(
            parse_resource_definition("gpu=range(10-123)"),
            "gpu",
            res_kind_range(10, 123),
        );
    }

    #[test]
    fn test_parse_resource_def_range_whitespace() {
        check_item(
            parse_resource_definition("  gpu  =  range  ( 10 -  123 ) "),
            "gpu",
            res_kind_range(10, 123),
        );
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
        Unexpected token found while attempting to parse number, expected - or _:
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
        check_item(
            parse_resource_definition("mem=sum(1_3000_2000)"),
            "mem",
            res_kind_sum(1_3000_2000),
        );
    }

    #[test]
    fn test_parse_resource_def_sum_frac() {
        check_item(
            parse_resource_definition("mem=sum(1.5)"),
            "mem",
            ResourceDescriptorKind::Sum {
                size: ResourceAmount::new(1, 5000),
            },
        );
    }

    #[test]
    fn test_parse_resource_def_sum_whitespace() {
        check_item(
            parse_resource_definition("   mem  = sum ( 1_3000_2000 ) "),
            "mem",
            res_kind_sum(1_3000_2000),
        );
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
        Unexpected end of input found while attempting to parse number, expected [ or range or sum:
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
        Unexpected end of input found while attempting to parse number, expected _ or x:
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

    #[test]
    fn test_parse_resource_coupling() {
        let resources = vec![
            ResourceDescriptorItem {
                name: "foo".to_string(),
                kind: ResourceDescriptorKind::Groups { groups: vec![] },
            },
            ResourceDescriptorItem {
                name: "bar".to_string(),
                kind: ResourceDescriptorKind::Groups { groups: vec![] },
            },
            ResourceDescriptorItem {
                name: "baz".to_string(),
                kind: ResourceDescriptorKind::Groups { groups: vec![] },
            },
        ];
        let r = parse_resource_coupling(
            "foo[2]:bar[3], baz[123] : bar[0]=1, foo[2]:bar[4] = 64",
            &resources,
        )
        .unwrap();
        assert_eq!(
            r.weights,
            vec![
                ResourceDescriptorCouplingItem {
                    resource1_idx: 0,
                    group1_idx: 2.into(),
                    resource2_idx: 1,
                    group2_idx: 3.into(),
                    weight: 256,
                },
                ResourceDescriptorCouplingItem {
                    resource1_idx: 0,
                    group1_idx: 2.into(),
                    resource2_idx: 1,
                    group2_idx: 4.into(),
                    weight: 64,
                },
                ResourceDescriptorCouplingItem {
                    resource1_idx: 1,
                    group1_idx: 0.into(),
                    resource2_idx: 2,
                    group2_idx: 123.into(),
                    weight: 1,
                },
            ]
        )
    }

    fn check_item(
        result: anyhow::Result<ResourceDescriptorItem>,
        name: &str,
        kind: ResourceDescriptorKind,
    ) {
        let result = result.unwrap();
        assert_eq!(result.name, name);
        assert_eq!(result.kind, kind);
    }

    fn check_kind(
        result: anyhow::Result<ResourceDescriptorKind>,
        expected: ResourceDescriptorKind,
    ) {
        assert_eq!(result.unwrap(), expected);
    }
}
