use anyhow::anyhow;
use nom::bytes::complete::tag;
use nom::combinator::all_consuming;
use nom::combinator::{map_res, opt};
use nom::multi::separated_list1;
use nom::sequence::{preceded, tuple};

use crate::common::arraydef::{ArrayDef, TaskIdRange};
use crate::common::parser::{p_uint, NomResult};
use crate::Set;

fn p_task_id_range(input: &str) -> NomResult<TaskIdRange> {
    map_res(
        tuple((
            p_uint,
            opt(preceded(tag("-"), p_uint)),
            opt(preceded(tag(":"), p_uint)),
        )),
        |r| match r {
            (v, None, None) => Ok(TaskIdRange::new(v, 1, 1)),
            (v, Some(w), None) if w >= v => Ok(TaskIdRange::new(v, w - v + 1, 1)),
            (v, Some(w), Some(x)) if w >= v && x <= w - v && x > 0 => {
                Ok(TaskIdRange::new(v, w - v + 1, x))
            }
            _ => Err(anyhow!("Invalid range")),
        },
    )(input)
}

fn p_array_def(input: &str) -> NomResult<ArrayDef> {
    map_res(p_task_id_ranges, |r| match r {
        res if !is_overlapping(res.clone()) => Ok(ArrayDef::new(res)),
        _ => Err("Ranges overlap"),
    })(input)
}

fn p_task_id_ranges(input: &str) -> NomResult<Vec<TaskIdRange>> {
    separated_list1(tag(","), p_task_id_range)(input)
}

pub fn parse_array_def(input: &str) -> anyhow::Result<ArrayDef> {
    all_consuming(p_array_def)(input)
        .map(|r| r.1)
        .map_err(format_parse_error)
}

fn is_overlapping(mut ranges: Vec<TaskIdRange>) -> bool {
    ranges.sort_unstable_by_key(|range| range.start);
    let mut ids = Set::new();
    for range in ranges {
        if range.iter().any(|x| !ids.insert(x)) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_uint() {
        assert_eq!(all_consuming(p_uint)("1019").unwrap().1, 1019);
        assert_eq!(all_consuming(p_uint)("0").unwrap().1, 0);
        assert!(all_consuming(p_uint)("").is_err());
        assert!(all_consuming(p_uint)("x").is_err());
    }

    #[test]
    fn test_parse_array_def() {
        assert_eq!(
            parse_array_def("34").unwrap().iter().collect::<Vec<_>>(),
            vec![34]
        );
        assert_eq!(
            parse_array_def("34-40").unwrap().iter().collect::<Vec<_>>(),
            vec![34, 35, 36, 37, 38, 39, 40]
        );
        assert_eq!(
            parse_array_def("101-101")
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![101]
        );
        assert!(parse_array_def("101-100").is_err());
        //assert_eq!(all_consuming(uint)("0").unwrap().1, 0);
    }

    #[test]
    fn test_parse_arrays_def() {
        assert_eq!(
            parse_array_def("34,35,36")
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![34, 35, 36]
        );
        assert_eq!(
            parse_array_def("34-40,45")
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![34, 35, 36, 37, 38, 39, 40, 45]
        );
        assert_eq!(
            parse_array_def("0-10:2")
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![0, 2, 4, 6, 8, 10]
        );
        assert!(parse_array_def("0-10, 5").is_err());
    }
}
