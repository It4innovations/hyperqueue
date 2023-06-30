use anyhow::anyhow;
use nom::character::complete::{newline, satisfy, space0};
use nom::combinator::{map, map_res, opt};
use nom::multi::{many1, separated_list1};
use nom::sequence::{preceded, terminated, tuple};
use nom::Parser;
use nom_supreme::tag::complete::tag;

use tako::hwstats::GpuFamily;
use tako::internal::has_unique_elements;
use tako::resources::{
    ResourceDescriptorItem, ResourceDescriptorKind, ResourceIndex, ResourceLabel,
    AMD_GPU_RESOURCE_NAME, MEM_RESOURCE_NAME, NVIDIA_GPU_RESOURCE_NAME,
};
use tako::{format_comma_delimited, Set};

use crate::common::format::human_size;
use crate::common::parser::{consume_all, p_u32, NomResult};

pub fn detect_cpus() -> anyhow::Result<ResourceDescriptorKind> {
    read_linux_numa()
        .map(|numa_nodes| {
            let filtered = filter_masked_cpus(numa_nodes.clone());
            if filtered.iter().flatten().count() != numa_nodes.iter().flatten().count() {
                log::info!(
                    "Some cores were filtered by a CPU mask. All cores: {:?}. Allowed cores: {:?}.",
                    numa_nodes
                        .iter()
                        .map(|c| format_comma_delimited(c.iter().map(|c| c.as_num())))
                        .collect::<Vec<_>>(),
                    filtered
                        .iter()
                        .map(|c| format_comma_delimited(c.iter().map(|c| c.as_num())))
                        .collect::<Vec<_>>()
                );
            }
            filtered
        })
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

/// Filter cores that are not allowed because of CPU affinity mask.
fn filter_masked_cpus(numa_nodes: Vec<Vec<ResourceIndex>>) -> Vec<Vec<ResourceIndex>> {
    match core_affinity::get_core_ids() {
        Some(allowed) => {
            let cpu_set: Set<usize> = allowed.into_iter().map(|core_id| core_id.id).collect();
            numa_nodes
                .into_iter()
                .map(|mut numa_node| {
                    numa_node.retain(|&cpu| cpu_set.contains(&cpu.into()));
                    numa_node
                })
                .collect()
        }
        None => {
            log::error!("Failed to found CPU mask. Allowing all cores.");
            numa_nodes
        }
    }
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

/// Detects additional resources (apart from CPU) on this worker.
/// Also returns the detected GPU families.
pub fn detect_additional_resources(
    items: &mut Vec<ResourceDescriptorItem>,
) -> anyhow::Result<Set<GpuFamily>> {
    let mut gpu_families = Set::new();
    let has_resource =
        |items: &[ResourceDescriptorItem], name: &str| items.iter().any(|x| x.name == name);

    let detected_gpus = detect_gpus_from_env();
    if detected_gpus.is_empty() && !has_resource(items, NVIDIA_GPU_RESOURCE_NAME) {
        if let Ok(count) = read_nvidia_linux_gpu_count() {
            if count > 0 {
                gpu_families.insert(GpuFamily::Nvidia);
                log::info!("Detected {} GPUs from procs", count);
                items.push(ResourceDescriptorItem {
                    name: NVIDIA_GPU_RESOURCE_NAME.to_string(),
                    kind: ResourceDescriptorKind::simple_indices(count as u32),
                });
            }
        }
    } else {
        for gpu in detected_gpus {
            if !has_resource(items, gpu.resource_name) {
                gpu_families.insert(gpu.family);
                items.push(ResourceDescriptorItem {
                    name: gpu.resource_name.to_string(),
                    kind: gpu.resource,
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
    Ok(gpu_families)
}

/// GPU resource that can be detected from an environment variable.
pub struct GpuEnvironmentRecord {
    env_var: &'static str,
    pub resource_name: &'static str,
    pub family: GpuFamily,
}

impl GpuEnvironmentRecord {
    const fn new(env_var: &'static str, resource_name: &'static str, family: GpuFamily) -> Self {
        Self {
            env_var,
            resource_name,
            family,
        }
    }
}

pub const GPU_ENVIRONMENTS: &[GpuEnvironmentRecord; 2] = &[
    GpuEnvironmentRecord::new(
        "CUDA_VISIBLE_DEVICES",
        NVIDIA_GPU_RESOURCE_NAME,
        GpuFamily::Nvidia,
    ),
    GpuEnvironmentRecord::new(
        "ROCR_VISIBLE_DEVICES",
        AMD_GPU_RESOURCE_NAME,
        GpuFamily::Amd,
    ),
];

struct DetectedGpu {
    resource_name: &'static str,
    resource: ResourceDescriptorKind,
    family: GpuFamily,
}

/// Tries to detect available GPUs from one of the `GPU_ENV_KEYS` environment variables.
fn detect_gpus_from_env() -> Vec<DetectedGpu> {
    let mut gpus = Vec::new();
    for gpu_env in GPU_ENVIRONMENTS {
        if let Ok(devices_str) = std::env::var(gpu_env.env_var) {
            if let Ok(devices) = parse_comma_separated_values(&devices_str) {
                log::info!(
                    "Detected GPUs {} from `{}`",
                    format_comma_delimited(&devices),
                    gpu_env.env_var,
                );

                if !has_unique_elements(&devices) {
                    log::warn!("{} contains duplicates ({devices_str})", gpu_env.env_var);
                    continue;
                }

                let list =
                    ResourceDescriptorKind::list(devices).expect("List values were not unique");
                gpus.push(DetectedGpu {
                    resource_name: gpu_env.resource_name,
                    resource: list,
                    family: gpu_env.family,
                });
            }
        }
    }
    gpus
}

/// Try to find out how many Nvidia GPUs are available on the current node.
fn read_nvidia_linux_gpu_count() -> anyhow::Result<usize> {
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
    use tako::AsIdVec;

    use super::{parse_range, read_linux_numa};

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
