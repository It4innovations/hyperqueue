use nom::character::complete::digit1;
use nom::combinator::map_res;
use nom::error::VerboseError;
use nom::IResult;

pub type NomResult<'a, Ret> = IResult<&'a str, Ret, VerboseError<&'a str>>;

pub fn p_uint(input: &str) -> NomResult<u32> {
    map_res(digit1, |digit_str: &str| digit_str.parse::<u32>())(input)
}
