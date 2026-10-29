use anyhow::{Context, anyhow};
use nom::Parser;
use nom::character::complete::{newline, satisfy, space0};
use nom::combinator::{map, map_res, opt};
use nom::multi::{many1, separated_list1};
use nom::sequence::{preceded, terminated, tuple};
use nom_supreme::tag::complete::tag;

use crate::client::commands::worker::{ResourceDetectComponent, ResourceDetectComponents};
use crate::common::format::human_size;
use crate::common::manager::info::ManagerInfo;
use crate::common::numa::GpuPciInfo;
use crate::common::numa::GpuPciInfo;
use crate::common::parser::{NomResult, consume_all, p_u32};
use tako::hwstats::GpuFamily;
use tako::internal::has_unique_elements;
use tako::resources::{
    AMD_GPU_RESOURCE_NAME, AMD_GPU_VENDOR_ID, CPU_RESOURCE_NAME, FRACTIONS_PER_UNIT, MEM_RESOURCE_NAME, NVIDIA_GPU_RESOURCE_NAME, NVIDIA_GPU_VENDOR_ID, ResourceAmount, ResourceDescriptorItem, ResourceDescriptorKind, ResourceFractions, ResourceIndex, ResourceLabel
};
use tako::{Set, format_comma_delimited};

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
            log::debug!("Detecting linux failed: {e}");
            let n_cpus = std::thread::available_parallelism().context("CPU detection failed")?;
            Ok(ResourceDescriptorKind::simple_indices(n_cpus.get() as u32))
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
                    numa_node.retain(|&cpu| cpu_set.contains(&cpu.as_usize()));
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
    detect_resources: &ResourceDetectComponents,
    manager_info: Option<&ManagerInfo>,
) -> anyhow::Result<Set<GpuFamily>> {
    let mut gpu_families = Set::new();
    let has_resource =
        |items: &[ResourceDescriptorItem], name: &str| items.iter().any(|x| x.name == name);

    let cpu_item = items.iter().find(|x| x.name == CPU_RESOURCE_NAME);
    let numa_topology = crate::common::numa::detect_numa_topology();
    let num_numa_nodes = match cpu_item {
        Some(item) if item.kind.n_groups() > 1 => item.kind.n_groups(),
        Some(_) => 1,
        None => numa_topology.nodes.len().max(1),
    };

    let numa_nodes_cpus: Vec<Vec<u32>> = if let Some(cpu_res) = cpu_item
        && let ResourceDescriptorKind::Groups { ref groups } = cpu_res.kind
    {
        groups
            .iter()
            .map(|g| {
                g.iter()
                    .filter_map(|label| label.parse::<u32>().ok())
                    .collect()
            })
            .collect()
    } else {
        numa_topology
            .nodes
            .iter()
            .map(|n| {
                parse_range(&n.cpulist)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|i| i.as_num())
                    .collect()
            })
            .collect()
    };

    let sys_nvidia_gpus: Vec<&GpuPciInfo> = numa_topology
        .gpus
        .iter()
        .filter(|g| g.vendor_id == NVIDIA_GPU_VENDOR_ID)
        .collect();

    let sys_amd_gpus: Vec<&GpuPciInfo> = numa_topology
        .gpus
        .iter()
        .filter(|g| g.vendor_id == AMD_GPU_VENDOR_ID)
        .collect();

    if detect_resources.has(ResourceDetectComponent::GpusNvidia)
        && !has_resource(items, NVIDIA_GPU_RESOURCE_NAME)
        && let Some(kind) = detect_gpus_for_family(
            GpuFamily::Nvidia,
            NVIDIA_GPU_RESOURCE_NAME,
            NVIDIA_GPU_ENVIRONMENT.env_var,
            num_numa_nodes,
            &sys_nvidia_gpus,
            &numa_nodes_cpus,
        )?
    {
        gpu_families.insert(GpuFamily::Nvidia);
        items.push(ResourceDescriptorItem {
            name: NVIDIA_GPU_RESOURCE_NAME.to_string(),
            kind,
        });
    }

    if detect_resources.has(ResourceDetectComponent::GpusAmd)
        && !has_resource(items, AMD_GPU_RESOURCE_NAME)
        && let Some(kind) = detect_gpus_for_family(
            GpuFamily::Amd,
            AMD_GPU_RESOURCE_NAME,
            AMD_GPU_ENVIRONMENT.env_var,
            num_numa_nodes,
            &sys_amd_gpus,
            &numa_nodes_cpus,
        )?
    {
        gpu_families.insert(GpuFamily::Amd);
        items.push(ResourceDescriptorItem {
            name: AMD_GPU_RESOURCE_NAME.to_string(),
            kind,
        });
        && let Some(kind) = detect_gpus_for_family(
            GpuFamily::Nvidia,
            NVIDIA_GPU_RESOURCE_NAME,
            NVIDIA_GPU_ENVIRONMENT.env_var,
            num_numa_nodes,
            &sys_nvidia_gpus,
            &numa_nodes_cpus,
        )?
    {
        gpu_families.insert(GpuFamily::Nvidia);
        items.push(ResourceDescriptorItem {
            name: NVIDIA_GPU_RESOURCE_NAME.to_string(),
            kind,
        });
    }

    if detect_resources.has(ResourceDetectComponent::GpusAmd)
        && !has_resource(items, AMD_GPU_RESOURCE_NAME)
        && let Some(kind) = detect_gpus_for_family(
            GpuFamily::Amd,
            AMD_GPU_RESOURCE_NAME,
            AMD_GPU_ENVIRONMENT.env_var,
            num_numa_nodes,
            &sys_amd_gpus,
            &numa_nodes_cpus,
        )?
    {
        gpu_families.insert(GpuFamily::Amd);
        items.push(ResourceDescriptorItem {
            name: AMD_GPU_RESOURCE_NAME.to_string(),
            kind,
        });
    }

    if !has_resource(items, MEM_RESOURCE_NAME)
        && detect_resources.has(ResourceDetectComponent::Memory)
    {
        // Note that memory sizes are always in mibibytes
        if let Some(mem) = manager_info.and_then(|i| i.max_memory_mb) {
            items.push(ResourceDescriptorItem {
                name: MEM_RESOURCE_NAME.to_string(),
                kind: ResourceDescriptorKind::Sum {
                    size: ResourceAmount::new(mem.try_into()?, 0),
                },
            });
        } else if let Ok(mem) = read_linux_memory() {
            log::info!("Detected {mem}B of memory ({})", human_size(mem));
            let units = mem / (1024 * 1024);
            let fractions = ((mem % (1024 * 1024)) * FRACTIONS_PER_UNIT as u64) / (1024 * 1024);
            items.push(ResourceDescriptorItem {
                name: MEM_RESOURCE_NAME.to_string(),
                kind: ResourceDescriptorKind::Sum {
                    size: ResourceAmount::new(units.try_into()?, fractions as ResourceFractions),
                },
            });
        }
    }
    Ok(gpu_families)
}

/// GPU resource that can be detected from an environment variable.
#[derive(Copy, Clone)]
pub struct GpuEnvironmentRecord {
    pub env_var: &'static str,
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

const NVIDIA_GPU_ENVIRONMENT: GpuEnvironmentRecord = GpuEnvironmentRecord::new(
    "CUDA_VISIBLE_DEVICES",
    NVIDIA_GPU_RESOURCE_NAME,
    GpuFamily::Nvidia,
);

const AMD_GPU_ENVIRONMENT: GpuEnvironmentRecord = GpuEnvironmentRecord::new(
    "ROCR_VISIBLE_DEVICES",
    AMD_GPU_RESOURCE_NAME,
    GpuFamily::Amd,
);

pub const GPU_ENVIRONMENTS: &[GpuEnvironmentRecord; 2] =
    &[NVIDIA_GPU_ENVIRONMENT, AMD_GPU_ENVIRONMENT];

/// Determines which NUMA node a GPU belongs to based on sysfs numa_node or local_cpulist.
fn determine_gpu_numa_node(
    gpu: &GpuPciInfo,
    num_numa_nodes: usize,
    numa_nodes_cpus: &[Vec<u32>],
) -> usize {
    if num_numa_nodes <= 1 {
        return 0;
    }

    // 1. Direct sysfs numa_node attribute
    if let Some(node_id) = gpu.numa_node
        && node_id >= 0
    {
        let node = node_id as usize;
        if node < num_numa_nodes {
            return node;
        }
        return node % num_numa_nodes;
    }

    // 2. Check local_cpulist attribute for overlap with node CPUs
    if let Some(ref cpulist_str) = gpu.local_cpulist
        && let Ok(local_cpus) = parse_range(cpulist_str)
        && !local_cpus.is_empty()
    {
        let mut best_node = 0;
        let mut max_overlap = 0;
        for (node_idx, node_cpus) in numa_nodes_cpus.iter().enumerate() {
            let overlap = local_cpus
                .iter()
                .filter(|c| node_cpus.contains(&c.as_num()))
                .count();
            if overlap > max_overlap {
                max_overlap = overlap;
                best_node = node_idx;
            }
        }
        if max_overlap > 0 {
            return best_node;
        }
    }

    0
}

/// Detects GPUs for a specific family (NVIDIA or AMD), grouping them by NUMA nodes.
fn detect_gpus_for_family(
    family: GpuFamily,
/// Determines which NUMA node a GPU belongs to based on sysfs numa_node or local_cpulist.
fn determine_gpu_numa_node(
    gpu: &GpuPciInfo,
    num_numa_nodes: usize,
    numa_nodes_cpus: &[Vec<u32>],
) -> usize {
    if num_numa_nodes <= 1 {
        return 0;
    }

    // 1. Direct sysfs numa_node attribute
    if let Some(node_id) = gpu.numa_node
        && node_id >= 0
    {
        let node = node_id as usize;
        if node < num_numa_nodes {
            return node;
        }
        return node % num_numa_nodes;
    }

    // 2. Check local_cpulist attribute for overlap with node CPUs
    if let Some(ref cpulist_str) = gpu.local_cpulist
        && let Ok(local_cpus) = parse_range(cpulist_str)
        && !local_cpus.is_empty()
    {
        let mut best_node = 0;
        let mut max_overlap = 0;
        for (node_idx, node_cpus) in numa_nodes_cpus.iter().enumerate() {
            let overlap = local_cpus
                .iter()
                .filter(|c| node_cpus.contains(&c.as_num()))
                .count();
            if overlap > max_overlap {
                max_overlap = overlap;
                best_node = node_idx;
            }
        }
        if max_overlap > 0 {
            return best_node;
        }
    }

    0
}

/// Detects GPUs for a specific family (NVIDIA or AMD), grouping them by NUMA nodes.
fn detect_gpus_for_family(
    family: GpuFamily,
    resource_name: &'static str,
    env_var: &'static str,
    num_numa_nodes: usize,
    sys_gpus: &[&GpuPciInfo],
    numa_nodes_cpus: &[Vec<u32>],
) -> anyhow::Result<Option<ResourceDescriptorKind>> {
    if let Ok(devices_str) = std::env::var(env_var) {
        let devices = match parse_comma_separated_values(&devices_str) {
            Ok(devs) => devs,
            Err(e) => {
                log::warn!("Failed to parse {env_var}: {e}");
                return Ok(None);
            }
        };
        if devices.is_empty() {
            return Ok(None);
        }
        if !has_unique_elements(&devices) {
            log::warn!("{env_var} contains duplicates ({devices_str})");
            return Ok(None);
        }
        log::info!(
            "Detected GPUs {} from `{env_var}`",
            format_comma_delimited(&devices),
        );

        let mut groups: Vec<Vec<ResourceLabel>> = vec![Vec::new(); num_numa_nodes];

        for label in devices {
            let node = if let Ok(idx) = label.parse::<usize>() {
                if let Some(gpu) = sys_gpus.get(idx) {
                    determine_gpu_numa_node(gpu, num_numa_nodes, numa_nodes_cpus)
                } else {
                    0
                }
            } else if let Some(gpu) = sys_gpus
                .iter()
                .find(|g| g.pci_address == label || g.pci_address.strip_prefix("0000:") == Some(label.as_str()))
            {
                determine_gpu_numa_node(gpu, num_numa_nodes, numa_nodes_cpus)
            } else {
                0
            };

            groups[node.min(num_numa_nodes - 1)].push(label);
        }

        let total = groups.iter().map(|g| g.len()).sum::<usize>();
        if total > 0 {
            let kind = ResourceDescriptorKind::groups(groups)
                .map_err(|e| anyhow!("Failed to create GPU resource descriptor for {resource_name}: {e:?}"))?;
            return Ok(Some(kind));
        }
        return Ok(None);
    }

    if !sys_gpus.is_empty() {
        let mut groups: Vec<Vec<ResourceLabel>> = vec![Vec::new(); num_numa_nodes];
        for (i, gpu) in sys_gpus.iter().enumerate() {
            let label = i.to_string();
            let node = determine_gpu_numa_node(gpu, num_numa_nodes, numa_nodes_cpus);
            groups[node.min(num_numa_nodes - 1)].push(label);
        }
        let kind = ResourceDescriptorKind::groups(groups)
            .map_err(|e| anyhow!("Failed to create GPU resource descriptor for {resource_name}: {e:?}"))?;
        return Ok(Some(kind));
    }

    if family == GpuFamily::Nvidia
        && let Ok(count) = read_nvidia_linux_gpu_count()
        && count > 0
    {
        log::info!("Detected {count} NVIDIA GPUs from procs");
        let mut groups: Vec<Vec<ResourceLabel>> = vec![Vec::new(); num_numa_nodes];
        for i in 0..count {
            groups[0].push(i.to_string());
        }
        let kind = ResourceDescriptorKind::groups(groups)
            .map_err(|e| anyhow!("Failed to create GPU resource descriptor for {resource_name}: {e:?}"))?;
        return Ok(Some(kind));
    }

    Ok(None)
}

/// Try to find out how many Nvidia GPUs are available on the current node.
fn read_nvidia_linux_gpu_count() -> anyhow::Result<usize> {
    Ok(std::fs::read_dir("/proc/driver/nvidia/gpus")?.count())
}

/// Try to get total memory on the current node.
fn read_linux_memory() -> anyhow::Result<u64> {
    let mut sys = sysinfo::System::new();
    sys.refresh_memory();
    Ok(sys.total_memory())
}

/// Try to find the CPU NUMA configuration.
///
/// Returns a list of NUMA nodes, each node contains a list of assigned CPUs.
fn read_linux_numa() -> anyhow::Result<Vec<Vec<ResourceIndex>>> {
    let nodes_str = std::fs::read_to_string("/sys/devices/system/node/online")
        .or_else(|_| std::fs::read_to_string("/sys/devices/system/node/possible"))?;
    let nodes = parse_range(&nodes_str)?;
    let mut numa_nodes: Vec<Vec<ResourceIndex>> = Vec::new();
    for numa_index in nodes {
        let filename = format!("/sys/devices/system/node/node{numa_index}/cpulist");
        if let Ok(content) = std::fs::read_to_string(&filename) {
            let trimmed = content.trim();
            if !trimmed.is_empty() {
                numa_nodes.push(parse_range(trimmed)?);
            }
        }
    }
    if numa_nodes.is_empty() {
        anyhow::bail!("No online NUMA nodes with CPUs found");
    }
    log::debug!("Linux numa detection is successful, found {} nodes", numa_nodes.len());
    Ok(numa_nodes)
}

fn read_linux_thread_siblings(cpu_id: &ResourceLabel) -> anyhow::Result<Vec<ResourceLabel>> {
    let filename = format!("/sys/devices/system/cpu/cpu{cpu_id}/topology/thread_siblings_list");
    log::debug!("Reading {filename}");
    parse_range(&std::fs::read_to_string(filename)?)
        .map(|indices| indices.into_iter().map(|i| i.to_string()).collect())
}

fn p_cpu_range(input: &str) -> NomResult<'_, Vec<ResourceIndex>> {
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

fn p_cpu_ranges(input: &str) -> NomResult<'_, Vec<ResourceIndex>> {
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
        assert_eq!(
            cpus.iter().map(|x| x.len()).sum::<usize>(),
            std::thread::available_parallelism().unwrap().get()
        );
    }

    #[test]
    fn test_determine_gpu_numa_node() {
        use super::determine_gpu_numa_node;
        use crate::common::numa::GpuPciInfo;

        let node_cpus = vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7]];

        // 1. Direct sysfs numa_node
        let gpu0 = GpuPciInfo {
            numa_node: Some(0),
            ..Default::default()
        };
        assert_eq!(determine_gpu_numa_node(&gpu0, 2, &node_cpus), 0);

        let gpu1 = GpuPciInfo {
            numa_node: Some(1),
            ..Default::default()
        };
        assert_eq!(determine_gpu_numa_node(&gpu1, 2, &node_cpus), 1);

        // 2. numa_node is -1, fallback to local_cpulist overlap
        let gpu_local1 = GpuPciInfo {
            numa_node: Some(-1),
            local_cpulist: Some("4-7".to_string()),
            ..Default::default()
        };
        assert_eq!(determine_gpu_numa_node(&gpu_local1, 2, &node_cpus), 1);

        let gpu_local0 = GpuPciInfo {
            numa_node: None,
            local_cpulist: Some("0-3".to_string()),
            ..Default::default()
        };
        assert_eq!(determine_gpu_numa_node(&gpu_local0, 2, &node_cpus), 0);

        // 3. Fallback when single NUMA node
        assert_eq!(determine_gpu_numa_node(&gpu1, 1, &node_cpus), 0);
    }

    #[test]
    fn test_detect_gpus_for_family_single_numa() {
        use super::detect_gpus_for_family;
        use crate::common::numa::GpuPciInfo;
        use tako::hwstats::GpuFamily;
        use tako::resources::ResourceDescriptorKind;

        let gpu0 = GpuPciInfo {
            pci_address: "0000:01:00.0".to_string(),
            vendor_id: 0x10de,
            numa_node: Some(0),
            ..Default::default()
        };
        let gpu1 = GpuPciInfo {
            pci_address: "0000:02:00.0".to_string(),
            vendor_id: 0x10de,
            numa_node: Some(0),
            ..Default::default()
        };
        let sys_gpus = vec![&gpu0, &gpu1];
        let node_cpus = vec![vec![0, 1, 2, 3]];

        let kind = detect_gpus_for_family(
            GpuFamily::Nvidia,
            "gpus/nvidia",
            "NON_EXISTENT_ENV_TEST",
            1,
            &sys_gpus,
            &node_cpus,
        )
        .unwrap()
        .unwrap();

        // On 1 NUMA node, it should be a List (n_groups == 1)
        assert_eq!(kind.n_groups(), 1);
        assert!(matches!(kind, ResourceDescriptorKind::List { .. }));
        if let ResourceDescriptorKind::List { values } = kind {
            assert_eq!(values, vec!["0", "1"]);
        }
    }

    #[test]
    fn test_detect_gpus_for_family_multi_numa_coupling() {
        use super::detect_gpus_for_family;
        use crate::common::numa::GpuPciInfo;
        use crate::worker::parser::parse_resource_coupling;
        use tako::hwstats::GpuFamily;
        use tako::resources::{
            ResourceDescriptor, ResourceDescriptorItem, ResourceDescriptorKind,
        };

        let gpu0 = GpuPciInfo {
            pci_address: "0000:01:00.0".to_string(),
            vendor_id: 0x10de,
            numa_node: Some(0),
            ..Default::default()
        };
        let gpu1 = GpuPciInfo {
            pci_address: "0000:81:00.0".to_string(),
            vendor_id: 0x10de,
            numa_node: Some(1),
            ..Default::default()
        };
        let sys_gpus = vec![&gpu0, &gpu1];
        let node_cpus = vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7]];

        let kind = detect_gpus_for_family(
            GpuFamily::Nvidia,
            "gpus/nvidia",
            "NON_EXISTENT_ENV_TEST",
            2,
            &sys_gpus,
            &node_cpus,
        )
        .unwrap()
        .unwrap();

        // On 2 NUMA nodes, it should be Groups with 2 groups
        assert_eq!(kind.n_groups(), 2);
        if let ResourceDescriptorKind::Groups { ref groups } = kind {
            assert_eq!(groups.len(), 2);
            assert_eq!(groups[0], vec!["0"]);
            assert_eq!(groups[1], vec!["1"]);
        } else {
            panic!("Expected ResourceDescriptorKind::Groups");
        }

        // Now test that coupling between cpus and gpus/nvidia works and validates
        let mut resources = vec![
            ResourceDescriptorItem {
                name: "cpus".to_string(),
                kind: ResourceDescriptorKind::groups(vec![
                    vec!["0".to_string(), "1".to_string()],
                    vec!["2".to_string(), "3".to_string()],
                ])
                .unwrap(),
            },
            ResourceDescriptorItem {
                name: "gpus/nvidia".to_string(),
                kind,
            },
        ];
        resources.sort_by(|a, b| a.name.cmp(&b.name));

        let coupling = parse_resource_coupling(
            "cpus[0]:gpus/nvidia[0]=256, cpus[1]:gpus/nvidia[1]=256",
            &resources,
        )
        .unwrap();

        let desc = ResourceDescriptor::new(resources, coupling);
        assert!(desc.validate(true).is_ok());
    }

    #[test]
    fn test_detect_gpus_for_family_asymmetric_numa() {
        use super::detect_gpus_for_family;
        use crate::common::numa::GpuPciInfo;
        use crate::worker::parser::parse_resource_coupling;
        use tako::hwstats::GpuFamily;
        use tako::resources::{
            ResourceDescriptor, ResourceDescriptorItem, ResourceDescriptorKind,
        };

        // Both GPUs attached to node 0, none on node 1
        let gpu0 = GpuPciInfo {
            pci_address: "0000:01:00.0".to_string(),
            vendor_id: 0x10de,
            numa_node: Some(0),
            ..Default::default()
        };
        let gpu1 = GpuPciInfo {
            pci_address: "0000:02:00.0".to_string(),
            vendor_id: 0x10de,
            numa_node: Some(0),
            ..Default::default()
        };
        let sys_gpus = vec![&gpu0, &gpu1];
        let node_cpus = vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7]];

        let kind = detect_gpus_for_family(
            GpuFamily::Nvidia,
            "gpus/nvidia",
            "NON_EXISTENT_ENV_TEST",
            2,
            &sys_gpus,
            &node_cpus,
        )
        .unwrap()
        .unwrap();

        assert_eq!(kind.n_groups(), 2);
        if let ResourceDescriptorKind::Groups { ref groups } = kind {
            assert_eq!(groups.len(), 2);
            assert_eq!(groups[0], vec!["0", "1"]);
            assert!(groups[1].is_empty());
        } else {
            panic!("Expected ResourceDescriptorKind::Groups");
        }

        let mut resources = vec![
            ResourceDescriptorItem {
                name: "cpus".to_string(),
                kind: ResourceDescriptorKind::groups(vec![
                    vec!["0".to_string(), "1".to_string()],
                    vec!["2".to_string(), "3".to_string()],
                ])
                .unwrap(),
            },
            ResourceDescriptorItem {
                name: "gpus/nvidia".to_string(),
                kind,
            },
        ];
        resources.sort_by(|a, b| a.name.cmp(&b.name));

        let coupling = parse_resource_coupling("cpus[0]:gpus/nvidia[0]=256", &resources).unwrap();
        let desc = ResourceDescriptor::new(resources, coupling);
        assert!(desc.validate(true).is_ok());
    }

    #[test]
    fn test_detect_gpus_for_family_pci_matching() {
        use super::detect_gpus_for_family;
        use crate::common::numa::GpuPciInfo;
        use tako::hwstats::GpuFamily;
        use tako::resources::ResourceDescriptorKind;

        let gpu0 = GpuPciInfo {
            pci_address: "0000:02:00.0".to_string(),
            vendor_id: 0x10de,
            numa_node: Some(0),
            ..Default::default()
        };
        let gpu1 = GpuPciInfo {
            pci_address: "0000:82:00.0".to_string(),
            vendor_id: 0x10de,
            numa_node: Some(1),
            ..Default::default()
        };
        let sys_gpus = vec![&gpu0, &gpu1];
        let node_cpus = vec![vec![0, 1], vec![2, 3]];

        // Test matching with BDF notation without domain (e.g. "82:00.0")
        let var_name = "TEST_CUDA_VISIBLE_DEVICES";
        unsafe {
            std::env::set_var(var_name, "82:00.0,0000:02:00.0");
        }
        let kind = detect_gpus_for_family(
            GpuFamily::Nvidia,
            "gpus/nvidia",
            var_name,
            2,
            &sys_gpus,
            &node_cpus,
        )
        .unwrap()
        .unwrap();
        unsafe {
            std::env::remove_var(var_name);
        }

        if let ResourceDescriptorKind::Groups { ref groups } = kind {
            assert_eq!(groups[0], vec!["0000:02:00.0"]);
            assert_eq!(groups[1], vec!["82:00.0"]);
        } else {
            panic!("Expected ResourceDescriptorKind::Groups");
        }
    }
}
