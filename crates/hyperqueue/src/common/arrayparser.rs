use anyhow::anyhow;
use nom::character::complete::char;
use nom::combinator::{map_res, opt};
use nom::multi::separated_list1;
use nom::sequence::{preceded, tuple};
use nom::Parser;
use nom_supreme::ParserExt;

use crate::common::arraydef::{IntArray, IntRange};
use crate::common::parser::{consume_all, p_u32, NomResult};
use crate::Set;

fn p_range(input: &str) -> NomResult<IntRange> {
    map_res(
        tuple((
            p_u32,
            opt(preceded(char('-'), p_u32)),
            opt(preceded(char(':'), p_u32)),
        )),
        |r| match r {
            (v, None, None) => Ok(IntRange::new(v, 1, 1)),
            (v, Some(w), None) if w >= v => Ok(IntRange::new(v, w - v + 1, 1)),
            (v, Some(w), Some(x)) if w >= v && x <= w - v && x > 0 => {
                Ok(IntRange::new(v, w - v + 1, x))
            }
            _ => Err(anyhow!("Invalid range")),
        },
    )
    .context("Integer range")
    .parse(input)
}

fn p_ranges(input: &str) -> NomResult<Vec<IntRange>> {
    separated_list1(char(','), p_range)(input)
}

fn p_array(input: &str) -> NomResult<IntArray> {
    map_res(p_ranges, |r| match r {
        res if !is_overlapping(res.clone()) => Ok(IntArray::new(res)),
        _ => Err(anyhow!("Ranges overlap")),
    })(input)
}

fn is_overlapping(mut ranges: Vec<IntRange>) -> bool {
    ranges.sort_unstable_by_key(|range| range.start);
    let mut ids = Set::new();
    for range in ranges {
        if range.iter().any(|x| !ids.insert(x)) {
            return true;
        }
    }
    false
}

pub fn parse_array(input: &str) -> anyhow::Result<IntArray> {
    consume_all(p_array, input)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_array_def() {
        assert_eq!(
            parse_array("34").unwrap().iter().collect::<Vec<_>>(),
            vec![34]
        );
        assert_eq!(
            parse_array("34-40").unwrap().iter().collect::<Vec<_>>(),
            vec![34, 35, 36, 37, 38, 39, 40]
        );
        assert_eq!(
            parse_array("101-101").unwrap().iter().collect::<Vec<_>>(),
            vec![101]
        );
        assert!(parse_array("101-100").is_err());
    }

    #[test]
    fn test_parse_arrays_def() {
        assert_eq!(
            parse_array("34,35,36").unwrap().iter().collect::<Vec<_>>(),
            vec![34, 35, 36]
        );
        assert_eq!(
            parse_array("34-40,45").unwrap().iter().collect::<Vec<_>>(),
            vec![34, 35, 36, 37, 38, 39, 40, 45]
        );
        assert_eq!(
            parse_array("0-10:2").unwrap().iter().collect::<Vec<_>>(),
            vec![0, 2, 4, 6, 8, 10]
        );
        assert!(parse_array("0-10, 5").is_err());
    }
}
