use crate::common::parser::{consume_all, p_u32, NomResult};
use anyhow::anyhow;

use crate::common::format::human_size;
use nom::character::complete::{newline, satisfy, space0};
use nom::combinator::{map, map_res, opt};
use nom::multi::{many1, separated_list1};
use nom::sequence::{preceded, terminated, tuple};
use nom::Parser;
use nom_supreme::tag::complete::tag;
use tako::format_comma_delimited;
use tako::internal::has_unique_elements;
use tako::resources::{
    ResourceDescriptorItem, ResourceDescriptorKind, ResourceIndex, ResourceLabel,
    GPU_RESOURCE_NAME, MEM_RESOURCE_NAME,
};

pub fn detect_cpus() -> anyhow::Result<ResourceDescriptorKind> {
    read_linux_numa()
        .and_then(|groups| {
            ResourceDescriptorKind::groups_numeric(groups)
                .map_err(|_| anyhow!("Inconsistent CPU naming got from detection"))
        })
        .or_else(|e| {
            log::debug!("Detecting linux failed: {}", e);
            let n_cpus = num_cpus::get() as u32;
            if n_cpus < 1 {
                anyhow::bail!("Cpu detection failed");
            };
            Ok(ResourceDescriptorKind::simple_indices(n_cpus))
        })
}

pub fn prune_hyper_threading(
    kind: &ResourceDescriptorKind,
) -> anyhow::Result<ResourceDescriptorKind> {
    let groups = kind.as_groups();
    let mut new_desc = Vec::new();
    for group in groups {
        let mut new_group = Vec::new();
        for cpu_id in group {
            if read_linux_thread_siblings(&cpu_id)?
                .iter()
                .min()
                .ok_or_else(|| anyhow::anyhow!("Thread siblings are empty"))
                .map(|v| *v == cpu_id)?
            {
                new_group.push(cpu_id);
            }
        }
        new_desc.push(new_group);
    }
    Ok(ResourceDescriptorKind::groups(new_desc).unwrap())
}

pub fn detect_additional_resources(items: &mut Vec<ResourceDescriptorItem>) -> anyhow::Result<()> {
    let has_resource =
        |items: &[ResourceDescriptorItem], name: &str| items.iter().any(|x| x.name == name);

    if !has_resource(items, GPU_RESOURCE_NAME) {
        if let Some(gpus) = detect_gpus_from_env() {
            items.push(ResourceDescriptorItem {
                name: GPU_RESOURCE_NAME.to_string(),
                kind: gpus,
            });
        } else if let Ok(count) = read_linux_gpu_count() {
            if count > 0 {
                log::info!("Detected {} GPUs from procs", count);
                items.push(ResourceDescriptorItem {
                    name: GPU_RESOURCE_NAME.to_string(),
                    kind: ResourceDescriptorKind::simple_indices(count as u32),
                });
            }
        }
    }

    if !has_resource(items, MEM_RESOURCE_NAME) {
        if let Ok(mem) = read_linux_memory() {
            log::info!("Detected {mem}B of memory ({})", human_size(mem));
            items.push(ResourceDescriptorItem {
                name: MEM_RESOURCE_NAME.to_string(),
                kind: ResourceDescriptorKind::Sum { size: mem },
            });
        }
    }
    Ok(())
}

/// Tries to detect available Nvidia GPUs from the `CUDA_VISIBLE_DEVICES` environment variable.
fn detect_gpus_from_env() -> Option<ResourceDescriptorKind> {
    if let Ok(devices_str) = std::env::var("CUDA_VISIBLE_DEVICES") {
        if let Ok(devices) = parse_comma_separated_values(&devices_str) {
            log::info!(
                "Detected GPUs {} from `CUDA_VISIBLE_DEVICES`",
                format_comma_delimited(&devices)
            );

            if !has_unique_elements(&devices) {
                log::warn!("CUDA_VISIBLE_DEVICES contains duplicates ({})", devices_str);
            }

            let list = ResourceDescriptorKind::list(devices).expect("List values were not unique");
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
fn read_linux_numa() -> anyhow::Result<Vec<Vec<ResourceIndex>>> {
    let nodes = parse_range(&std::fs::read_to_string(
        "/sys/devices/system/node/possible",
    )?)?;
    let mut numa_nodes: Vec<Vec<ResourceIndex>> = Vec::new();
    for numa_index in nodes {
        let filename = format!("/sys/devices/system/node/node{numa_index}/cpulist");
        numa_nodes.push(parse_range(&std::fs::read_to_string(filename)?)?);
    }
    log::debug!("Linux numa detection is successful");
    Ok(numa_nodes)
}

fn read_linux_thread_siblings(cpu_id: &ResourceLabel) -> anyhow::Result<Vec<ResourceLabel>> {
    let filename = format!(
        "/sys/devices/system/cpu/cpu{}/topology/thread_siblings_list",
        cpu_id
    );
    log::debug!("Reading {}", filename);
    parse_range(&std::fs::read_to_string(filename)?)
        .map(|indices| indices.into_iter().map(|i| i.to_string()).collect())
}

fn p_cpu_range(input: &str) -> NomResult<Vec<ResourceIndex>> {
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

fn p_cpu_ranges(input: &str) -> NomResult<Vec<ResourceIndex>> {
    separated_list1(terminated(tag(","), space0), p_cpu_range)(input)
        .map(|(a, b)| (a, b.into_iter().flatten().collect()))
}

fn parse_range(input: &str) -> anyhow::Result<Vec<ResourceIndex>> {
    let parser = terminated(p_cpu_ranges, opt(newline));
    consume_all(parser, input)
}

fn parse_comma_separated_values(input: &str) -> anyhow::Result<Vec<String>> {
    let any_except_comma = map(many1(satisfy(|c| c != ',')), |items| {
        items.into_iter().collect::<String>()
    });

    consume_all(separated_list1(tag(","), any_except_comma), input)
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
