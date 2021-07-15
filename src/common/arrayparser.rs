use anyhow::anyhow;
use nom::bytes::complete::tag;
use nom::combinator::all_consuming;
use nom::combinator::{map, map_res, opt};
use nom::sequence::{preceded, tuple};

use crate::common::arraydef::{ArrayDef, TaskIdRange};
use crate::common::parser::{format_parse_error, p_uint, NomResult};

fn p_task_id_range(input: &str) -> NomResult<TaskIdRange> {
    map_res(
        tuple((p_uint, opt(preceded(tag("-"), p_uint)))),
        |r| match r {
            (v, None) => Ok(TaskIdRange::new(v, 1)),
            (v, Some(w)) if w >= v => Ok(TaskIdRange::new(v, w - v + 1)),
            _ => Err(anyhow!("Invalid range")),
        },
    )(input)
}

fn p_array_def(input: &str) -> NomResult<ArrayDef> {
    map(p_task_id_range, ArrayDef::new)(input)
}

pub fn parse_array_def(input: &str) -> anyhow::Result<ArrayDef> {
    all_consuming(p_array_def)(input)
        .map(|r| r.1)
        .map_err(format_parse_error)
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
}
