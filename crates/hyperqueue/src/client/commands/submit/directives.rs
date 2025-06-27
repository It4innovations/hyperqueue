use anyhow::Context;
use std::fs::File;
use std::path::Path;

use bstr::{BStr, BString, ByteSlice};
use clap::{CommandFactory, FromArgMatches};
use nom::branch::alt;
use nom::bytes::complete::escaped;
use nom::character::complete::{char, space1};
use nom::character::complete::{none_of, one_of};
use nom::combinator::{cut, map, opt};
use nom::multi::separated_list0;
use nom::sequence::{preceded, terminated, tuple};

use crate::client::commands::submit::SubmitJobTaskConfOpts;
use crate::common::cli::OptsWithMatches;
use crate::common::parser::{NomResult, consume_all};
use crate::common::utils::fs::read_at_most;

const MAX_PREFIX_OF_SUBMIT_SCRIPT: usize = 32 * 1024; // 32KiB

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

pub struct Shebang(Vec<String>);

impl Shebang {
    pub fn modify_commands(self, commands: Vec<String>) -> Vec<String> {
        let mut args = self.0;
        args.extend(commands);
        args
    }
}

struct FileMetadata {
    shebang: Option<Shebang>,
    directives: Vec<String>,
}

fn extract_metadata(data: &BStr) -> crate::Result<FileMetadata> {
    let mut shebang = None;

    let mut directives = Vec::new();
    for (index, line) in data.lines().enumerate() {
        let line = line.trim_start();
        if line.starts_with(b"#HQ ") {
            let value = &line[4..].trim();
            if !value.is_empty() {
                directives.push(String::from_utf8_lossy(value).to_string());
            }
            continue;
        } else if index == 0 && line.starts_with(b"#!") {
            let shebang_line = String::from_utf8_lossy(line[2..].trim());
            let args = parse_args(&shebang_line).context("Cannot parse shebang line")?;
            shebang = Some(Shebang(args));
        }

        match line.first() {
            Some(b'#') | None => continue,
            _ => break,
        }
    }
    Ok(FileMetadata {
        shebang,
        directives,
    })
}

pub fn parse_hq_directives_from_file(
    path: &Path,
) -> anyhow::Result<(OptsWithMatches<SubmitJobTaskConfOpts>, Option<Shebang>)> {
    log::debug!("Extracting directives from file: {}", path.display());

    let file = File::open(path)?;
    let buffer = read_at_most(file, MAX_PREFIX_OF_SUBMIT_SCRIPT)?;
    parse_hq_directives(&buffer)
}

pub fn parse_hq_directives(
    data: &[u8],
) -> anyhow::Result<(OptsWithMatches<SubmitJobTaskConfOpts>, Option<Shebang>)> {
    let prefix = BString::from(data);

    let metadata = extract_metadata(prefix.as_bstr())?;
    let mut arguments = Vec::new();
    for directive in metadata.directives {
        let mut args = parse_args(&directive)?;
        arguments.append(&mut args);
    }

    // clap parses first argument as name of the program
    arguments.insert(0, "".to_string());
    log::debug!("Applying directive(s): {arguments:?}");

    let app = SubmitJobTaskConfOpts::command()
        .disable_help_flag(true)
        .override_usage("#HQ <hq submit parameters>");
    let matches = app.try_get_matches_from(&arguments).map_err(|error| {
        anyhow::anyhow!(
            "You have used invalid parameter(s) after a #HQ directive.\n{}",
            error
        )
    })?;

    let opts = SubmitJobTaskConfOpts::from_arg_matches(&matches)?;
    let parsed = OptsWithMatches::new(opts, matches);

    Ok((parsed, metadata.shebang))
}

#[cfg(test)]
mod tests {
    use bstr::{BString, ByteSlice};

    use super::{extract_metadata, parse_args};

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
        let metadata = extract_metadata(data.as_bstr()).unwrap();
        assert_eq!(
            metadata.directives,
            vec![BString::from("--abc --xyz"), BString::from("next arg")]
        );
        assert_eq!(metadata.shebang.unwrap().0, vec!["/bin/bash".to_string()]);
    }

    #[test]
    fn test_parse_directives_shebang_with_args() {
        let data = BString::from(
            b"#!/bin/bash -l \"a b\"
# Comment
#HQ --abc --xyz
"
            .as_ref(),
        );
        let metadata = extract_metadata(data.as_bstr()).unwrap();
        assert_eq!(
            metadata.shebang.unwrap().0,
            vec!["/bin/bash".to_string(), "-l".to_string(), "a b".to_string()]
        );
    }

    #[test]
    fn test_parse_directives_no_shebang() {
        let data = BString::from(
            b"
# Comment
#HQ --abc --xyz
"
            .as_ref(),
        );
        let metadata = extract_metadata(data.as_bstr()).unwrap();
        assert!(metadata.shebang.is_none());
    }
}
