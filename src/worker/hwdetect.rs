use crate::common::parser::{format_parse_error, p_uint, NomResult};

use tako::common::resources::{CpuId, NumOfCpus, ResourceDescriptor};

use nom::bytes::complete::tag;
use nom::character::complete::{newline, space0};
use nom::combinator::all_consuming;
use nom::combinator::{map_res, opt};
use nom::multi::separated_list1;
use nom::sequence::{preceded, terminated, tuple};

fn p_cpu_range(input: &str) -> NomResult<Vec<CpuId>> {
    map_res(
        tuple((
            terminated(p_uint, space0),
            opt(terminated(
                preceded(tuple((tag("-"), space0)), p_uint),
                space0,
            )),
        )),
        |(u, v)| crate::Result::Ok((u..=v.unwrap_or(u)).collect()),
    )(input)
}

fn p_cpu_ranges(input: &str) -> NomResult<Vec<CpuId>> {
    separated_list1(terminated(tag(","), space0), p_cpu_range)(input)
        .map(|(a, b)| (a, b.into_iter().flatten().collect()))
}

pub fn parse_range(input: &str) -> anyhow::Result<Vec<CpuId>> {
    all_consuming(terminated(p_cpu_ranges, opt(newline)))(input)
        .map(|r| r.1)
        .map_err(format_parse_error)
}

pub fn read_linux_numa() -> anyhow::Result<Vec<Vec<CpuId>>> {
    let nodes = parse_range(&std::fs::read_to_string(
        "/sys/devices/system/node/possible",
    )?)?;
    let mut cpus: Vec<Vec<CpuId>> = Vec::new();
    for numa_index in nodes {
        let filename = format!("/sys/devices/system/node/node{}/cpulist", numa_index);
        cpus.push(parse_range(&std::fs::read_to_string(filename)?)?);
    }
    Ok(cpus)
}

pub fn detect_resource() -> anyhow::Result<ResourceDescriptor> {
    if let Ok(cpus) = read_linux_numa() {
        log::debug!("Linux numa detection is successful");
        return Ok(ResourceDescriptor { cpus });
    }

    let n_cpus = num_cpus::get() as NumOfCpus;
    if n_cpus < 1 {
        anyhow::bail!("Cpu detection failed".to_string());
    };
    Ok(ResourceDescriptor::new_with_socket_size(1, n_cpus))
}

pub fn print_resource_descriptor(descriptor: &ResourceDescriptor) {
    println!("Summary: {}", descriptor.summary());
    println!("Cpu Ids: {}", descriptor.full_describe());
}

#[cfg(test)]
mod tests {
    use super::{parse_range, read_linux_numa};

    #[test]
    fn test_parse_range() {
        assert_eq!(parse_range("10").unwrap(), vec![10]);
        assert_eq!(parse_range("10\n").unwrap(), vec![10]);
        assert_eq!(parse_range("0-3\n").unwrap(), vec![0, 1, 2, 3]);
        assert_eq!(
            parse_range("111-115\n").unwrap(),
            vec![111, 112, 113, 114, 115]
        );
        assert_eq!(parse_range("2,7, 10").unwrap(), vec![2, 7, 10]);
        assert_eq!(
            parse_range("2-7,10-12,20").unwrap(),
            vec![2, 3, 4, 5, 6, 7, 10, 11, 12, 20]
        );
        assert!(parse_range("xx\n").is_err());
        assert!(parse_range("-\n").is_err());
        assert!(parse_range("-2\n").is_err());
        assert!(parse_range("0-1-2\n").is_err());
        assert!(parse_range(",,").is_err());
    }

    #[test]
    fn test_read_linux_numa() {
        let cpus = read_linux_numa().unwrap();
        assert_eq!(cpus.iter().map(|x| x.len()).sum::<usize>(), num_cpus::get());
    }
}
