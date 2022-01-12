use crate::client::commands::submit::SubmitJobConfOpts;
use crate::common::parser::{consume_all, NomResult};
use bstr::{BStr, BString, ByteSlice};
use clap::Parser;
use nom::branch::alt;
use nom::bytes::complete::escaped;
use nom::character::complete::{char, space1};
use nom::character::complete::{none_of, one_of};
use nom::combinator::{cut, map, opt};
use nom::multi::separated_list0;
use nom::sequence::{preceded, terminated, tuple};
use std::fs::File;
use std::io::Read;
use std::path::Path;

const MAX_PREFIX_OF_SCRIPT: usize = 32 * 1024; // 32KiB

fn p_double_quoted(input: &str) -> NomResult<&str> {
    preceded(
        char('"'),
        cut(terminated(
            escaped(none_of("\"\\"), '\\', one_of("\"")),
            char('"'),
        )),
    )(input)
}

fn p_arg(input: &str) -> NomResult<String> {
    alt((
        map(p_double_quoted, |s| s.to_string()),
        map(
            tuple((
                escaped(none_of(" \"\\"), '\\', one_of(" \"")),
                opt(p_double_quoted),
            )),
            |pair| match pair {
                (a, None) => a.to_string(),
                (a, Some(b)) => {
                    let mut s = a.to_string();
                    s.push_str(b);
                    s
                }
            },
        ),
    ))(input)
}

fn parse_args(input: &str) -> anyhow::Result<Vec<String>> {
    consume_all(separated_list0(space1, p_arg), input)
}

fn extract_directives(data: &BStr) -> crate::Result<Vec<String>> {
    let mut args = Vec::new();
    for line in data.lines() {
        let line = line.trim_start();
        if line.starts_with(b"#HQ ") {
            let value = &line[4..].trim();
            if !value.is_empty() {
                args.push(String::from_utf8_lossy(value).to_string());
            }
            continue;
        }
        match line.get(0) {
            Some(b'#') | None => continue,
            _ => break,
        }
    }
    Ok(args)
}

pub(crate) fn parse_hq_directives(
    path: &Path,
    opts: SubmitJobConfOpts,
) -> crate::Result<SubmitJobConfOpts> {
    log::debug!("Extracting directives from file");
    let mut f = File::open(&path)?;
    let mut buffer = [0; MAX_PREFIX_OF_SCRIPT];
    let size = f.read(&mut buffer)?;
    let prefix = BString::from(&buffer[..size]);
    let mut directives = Vec::new();
    for directive in extract_directives(prefix.as_bstr())? {
        let mut args = parse_args(&directive)?;
        directives.append(&mut args);
    }
    // clap parses first argument as name of the program
    directives.insert(0, "".to_string());
    log::debug!("Applying directive from file: {:?}", directives);
    match SubmitJobConfOpts::try_parse_from(&directives) {
        Ok(new) => Ok(opts.merge(new)),
        Err(e) => {
            log::error!("Error encountered while parsing #HQ directive in script");
            e.exit()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{extract_directives, parse_args};
    use bstr::{BString, ByteSlice};

    #[test]
    fn test_arg_parser() {
        assert_eq!(parse_args("abc!").unwrap(), vec!["abc!"]);
        assert_eq!(
            parse_args("one two   three").unwrap(),
            vec!["one", "two", "three"]
        );
        assert_eq!(
            parse_args("--cpus=\"2 compact\"").unwrap(),
            vec!["--cpus=2 compact"]
        );
        assert_eq!(
            parse_args("--name \"let's have a space inside\" --cpus \"2 compact\" --pin").unwrap(),
            vec![
                "--name",
                "let's have a space inside",
                "--cpus",
                "2 compact",
                "--pin"
            ]
        );
    }

    #[test]
    fn test_parse_directives() {
        let data = BString::from(
            b"#!/bin/bash

# Comment
#HQ --abc --xyz

#HQ 
#HQ next arg

sleep 1
#HQ this should be ignored
"
            .as_ref(),
        );
        assert_eq!(
            extract_directives(data.as_bstr()).unwrap(),
            vec![BString::from("--abc --xyz"), BString::from("next arg")]
        );
    }
}
