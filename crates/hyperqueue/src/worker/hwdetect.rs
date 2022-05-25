use crate::common::parser::{consume_all, p_u32, NomResult};

use crate::common::format::human_size;
use nom::character::complete::{newline, space0};
use nom::combinator::{map_res, opt};
use nom::multi::separated_list1;
use nom::sequence::{preceded, terminated, tuple};
use nom::Parser;
use nom_supreme::tag::complete::tag;
use tako::format_comma_delimited;
use tako::resources::cpu_descriptor_from_socket_size;
use tako::resources::{CpuId, CpusDescriptor, GenericResourceDescriptor, NumOfCpus};

pub const GPU_RESOURCE_NAME: &str = "gpus";
pub const MEM_RESOURCE_NAME: &str = "mem";

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

pub fn detect_cpus_no_ht() -> anyhow::Result<CpusDescriptor> {
    let descriptor = detect_cpus()?;
    let mut new_desc = Vec::new();
    for socket in descriptor {
        let mut new_socket = Vec::new();
        for cpu_id in socket {
            if read_linux_thread_siblings(cpu_id)?
                .iter()
                .min()
                .ok_or_else(|| anyhow::anyhow!("Thread siblings are empty"))
                .map(|v| *v == cpu_id)?
            {
                new_socket.push(cpu_id);
            }
        }
        new_desc.push(new_socket);
    }
    Ok(new_desc)
}

pub fn detect_generic_resources() -> anyhow::Result<Vec<GenericResourceDescriptor>> {
    let mut generic = Vec::new();

    if let Some(gpus) = detect_gpus_from_env() {
        generic.push(gpus);
    } else if let Ok(count) = read_linux_gpu_count() {
        if count > 0 {
            log::info!("Detected {} GPUs from procs", count);
            generic.push(GenericResourceDescriptor::range(
                GPU_RESOURCE_NAME,
                0,
                count as u32 - 1,
            ));
        }
    }

    if let Ok(mem) = read_linux_memory() {
        log::info!("Detected {mem}B of memory ({})", human_size(mem));
        generic.push(GenericResourceDescriptor::sum(MEM_RESOURCE_NAME, mem));
    }
    Ok(generic)
}

/// Tries to detect available Nvidia GPUs from the `CUDA_VISIBLE_DEVICES` environment variable.
fn detect_gpus_from_env() -> Option<GenericResourceDescriptor> {
    if let Ok(devices_str) = std::env::var("CUDA_VISIBLE_DEVICES") {
        if let Ok(mut devices) = parse_comma_separated_numbers(&devices_str) {
            log::info!(
                "Detected GPUs {} from `CUDA_VISIBLE_DEVICES`",
                format_comma_delimited(&devices)
            );

            let count = devices.len();
            devices.sort_unstable();
            devices.dedup();

            if count != devices.len() {
                log::warn!("CUDA_VISIBLE_DEVICES contains duplicates ({})", devices_str);
            }

            let list = GenericResourceDescriptor::list(GPU_RESOURCE_NAME, devices)
                .expect("List values were not unique");
            return Some(list);
        }
    }
    None
}

/// Try to find out how many Nvidia GPUs are available on the current node.
fn read_linux_gpu_count() -> anyhow::Result<usize> {
    Ok(std::fs::read_dir("/proc/driver/nvidia/gpus")?.count())
}

/// Try to get total memory on the current node.
fn read_linux_memory() -> anyhow::Result<u64> {
    Ok(psutil::memory::virtual_memory()?.total())
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

fn read_linux_thread_siblings(cpu_id: CpuId) -> anyhow::Result<Vec<CpuId>> {
    let filename = format!(
        "/sys/devices/system/cpu/cpu{}/topology/thread_siblings_list",
        cpu_id
    );
    log::debug!("Reading {}", filename);
    parse_range(&std::fs::read_to_string(filename)?)
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
    )
    .parse(input)
}

fn p_cpu_ranges(input: &str) -> NomResult<Vec<CpuId>> {
    separated_list1(terminated(tag(","), space0), p_cpu_range)(input)
        .map(|(a, b)| (a, b.into_iter().flatten().collect()))
}

fn parse_range(input: &str) -> anyhow::Result<Vec<CpuId>> {
    let parser = terminated(p_cpu_ranges, opt(newline));
    consume_all(parser, input)
}

fn parse_comma_separated_numbers(input: &str) -> anyhow::Result<Vec<u32>> {
    consume_all(separated_list1(tag(","), p_u32), input)
}

#[cfg(test)]
mod tests {
    use super::{parse_range, read_linux_numa};
    use tako::AsIdVec;

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
