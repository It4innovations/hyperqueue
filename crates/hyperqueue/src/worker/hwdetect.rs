use crate::common::parser::{format_parse_error, p_u32, NomResult};

use tako::common::resources::{
    CpuId, CpusDescriptor, GenericResourceDescriptor, GenericResourceDescriptorKind,
    GenericResourceIndex, NumOfCpus,
};

use nom::bytes::complete::tag;
use nom::character::complete::{newline, space0};
use nom::combinator::all_consuming;
use nom::combinator::{map_res, opt};
use nom::multi::separated_list1;
use nom::sequence::{preceded, terminated, tuple};
use tako::common::resources::descriptor::{
    cpu_descriptor_from_socket_size, GenericResourceKindIndices,
};

pub fn detect_cpus() -> anyhow::Result<CpusDescriptor> {
    read_linux_numa().or_else(|e| {
        log::debug!("Detecting linux failed: {}", e);
        let n_cpus = num_cpus::get() as NumOfCpus;
        if n_cpus < 1 {
            anyhow::bail!("Cpu detection failed".to_string());
        };
        Ok(cpu_descriptor_from_socket_size(1, n_cpus))
    })
}

pub fn detect_generic_resource() -> anyhow::Result<Vec<GenericResourceDescriptor>> {
    let mut generic = Vec::new();
    if let Ok(count) = read_linux_gpu_count() {
        if count > 0 {
            log::debug!("Gpus detected: {}", count);
            generic.push(GenericResourceDescriptor {
                name: "gpus".to_string(),
                kind: GenericResourceDescriptorKind::Indices(GenericResourceKindIndices {
                    start: GenericResourceIndex::new(0),
                    end: GenericResourceIndex::new(count as u32 - 1),
                }),
            })
        }
    }
    Ok(generic)
}

/// Try to find out how many Nvidia GPUs are available on the current node.
fn read_linux_gpu_count() -> anyhow::Result<usize> {
    Ok(std::fs::read_dir("/proc/driver/nvidia/gpus")?.count())
}

/// Try to find the CPU NUMA configuration.
///
/// Returns a list of NUMA nodes, each node contains a list of assigned CPUs.
fn read_linux_numa() -> anyhow::Result<Vec<Vec<CpuId>>> {
    let nodes = parse_range(&std::fs::read_to_string(
        "/sys/devices/system/node/possible",
    )?)?;
    let mut numa_nodes: Vec<Vec<CpuId>> = Vec::new();
    for numa_index in nodes {
        let filename = format!("/sys/devices/system/node/node{}/cpulist", numa_index);
        numa_nodes.push(parse_range(&std::fs::read_to_string(filename)?)?);
    }
    log::debug!("Linux numa detection is successful");
    Ok(numa_nodes)
}

fn p_cpu_range(input: &str) -> NomResult<Vec<CpuId>> {
    map_res(
        tuple((
            terminated(p_u32, space0),
            opt(terminated(
                preceded(tuple((tag("-"), space0)), p_u32),
                space0,
            )),
        )),
        |(u, v)| crate::Result::Ok((u..=v.unwrap_or(u)).map(|id| id.into()).collect()),
    )(input)
}

fn p_cpu_ranges(input: &str) -> NomResult<Vec<CpuId>> {
    separated_list1(terminated(tag(","), space0), p_cpu_range)(input)
        .map(|(a, b)| (a, b.into_iter().flatten().collect()))
}

fn parse_range(input: &str) -> anyhow::Result<Vec<CpuId>> {
    all_consuming(terminated(p_cpu_ranges, opt(newline)))(input)
        .map(|r| r.1)
        .map_err(format_parse_error)
}

#[cfg(test)]
mod tests {
    use super::{parse_range, read_linux_numa};
    use tako::common::index::AsIdVec;

    #[test]
    fn test_parse_range() {
        assert_eq!(parse_range("10").unwrap(), vec![10].to_ids());
        assert_eq!(parse_range("10\n").unwrap(), vec![10].to_ids());
        assert_eq!(parse_range("0-3\n").unwrap(), vec![0, 1, 2, 3].to_ids());
        assert_eq!(
            parse_range("111-115\n").unwrap(),
            vec![111, 112, 113, 114, 115].to_ids()
        );
        assert_eq!(parse_range("2,7, 10").unwrap(), vec![2, 7, 10].to_ids());
        assert_eq!(
            parse_range("2-7,10-12,20").unwrap(),
            vec![2, 3, 4, 5, 6, 7, 10, 11, 12, 20].to_ids()
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
