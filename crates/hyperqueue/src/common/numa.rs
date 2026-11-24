use std::fs;
use std::io::{self, BufRead};
use std::path::Path;

use serde::{Deserialize, Serialize};
use tako::resources::{
    CPU_RESOURCE_NAME, ResourceDescriptorCoupling, ResourceDescriptorCouplingItem,
    ResourceDescriptorItem,
};
use tako::worker::WorkerConfiguration;

pub const WORKER_EXTRA_NUMA_KEY: &str = "numa";

/// Information about a detected NUMA node on a worker.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct NumaNodeInfo {
    pub id: u32,
    pub cpulist: String,
    pub cpu_count: usize,
    pub mem_total_bytes: Option<u64>,
    pub mem_free_bytes: Option<u64>,
    pub distances: Vec<u32>,
}

/// Information about a detected GPU or accelerator device attached via PCI.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct GpuPciInfo {
    pub pci_address: String,
    pub vendor_id: u16,
    pub device_id: u16,
    pub class_id: u32,
    pub vendor_name: String,
    pub device_name: Option<String>,
    pub driver: Option<String>,
    pub numa_node: Option<i32>,
    pub local_cpulist: Option<String>,
}

/// Aggregated NUMA and accelerator device topology for a worker.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct NumaTopology {
    pub nodes: Vec<NumaNodeInfo>,
    pub gpus: Vec<GpuPciInfo>,
}

impl NumaTopology {
    pub fn total_cpus(&self) -> usize {
        self.nodes.iter().map(|n| n.cpu_count).sum()
    }

    pub fn total_memory_bytes(&self) -> Option<u64> {
        let mut total = 0u64;
        let mut has_mem = false;
        for node in &self.nodes {
            if let Some(mem) = node.mem_total_bytes {
                total = total.saturating_add(mem);
                has_mem = true;
            }
        }
        if has_mem { Some(total) } else { None }
    }

    pub fn gpus_for_node(&self, node_id: u32) -> Vec<&GpuPciInfo> {
        self.gpus
            .iter()
            .filter(|g| g.numa_node == Some(node_id as i32))
            .collect()
    }
}

pub trait GetNumaInfo {
    fn get_numa_info(&self) -> Option<NumaTopology>;
}

impl GetNumaInfo for WorkerConfiguration {
    fn get_numa_info(&self) -> Option<NumaTopology> {
        self.extra
            .get(WORKER_EXTRA_NUMA_KEY)
            .and_then(|str| serde_json::from_str(str).ok())
    }
}

/// Translates NUMA distances into `ResourceDescriptorCoupling` for the CPU resource groups.
///
/// Handles arbitrary NUMA topology architectures of variable size N x N:
/// - Single-node / UMA: returns empty coupling.
/// - Multi-node systems (2, 4, 8, 16, 32, 64 nodes etc.):
///   Dynamically calculates D_min (minimum non-self distance > 0 across pairs).
///   Scales each pair's weight inversely: W(D) = round(256 * D_min / D), clamped to [1, 256].
///   Edges where D <= 0 or missing are omitted.
pub fn numa_distances_to_coupling(
    topology: &NumaTopology,
    cpu_resource_idx: u8,
    num_cpu_groups: usize,
) -> ResourceDescriptorCoupling {
    if num_cpu_groups <= 1 || topology.nodes.len() <= 1 {
        return ResourceDescriptorCoupling::default();
    }

    // Match CPU groups to NUMA nodes that have CPUs
    let cpu_nodes: Vec<&NumaNodeInfo> = topology.nodes.iter().filter(|n| n.cpu_count > 0).collect();
    let nodes_ref: Vec<&NumaNodeInfo> = if cpu_nodes.len() == num_cpu_groups {
        cpu_nodes
    } else {
        // Fallback: use all nodes up to num_cpu_groups
        topology.nodes.iter().collect()
    };

    if nodes_ref.len() < num_cpu_groups {
        return ResourceDescriptorCoupling::default();
    }

    // Helper to find pairwise distance between group i and group j
    let get_distance = |i: usize, j: usize| -> Option<u32> {
        let node_i = nodes_ref.get(i)?;
        let node_j = nodes_ref.get(j)?;

        // Try lookup by hardware node ID first, then positional fallback
        let d_ij = node_i
            .distances
            .get(node_j.id as usize)
            .copied()
            .filter(|&d| d > 0)
            .or_else(|| node_i.distances.get(j).copied().filter(|&d| d > 0));

        let d_ji = node_j
            .distances
            .get(node_i.id as usize)
            .copied()
            .filter(|&d| d > 0)
            .or_else(|| node_j.distances.get(i).copied().filter(|&d| d > 0));

        match (d_ij, d_ji) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    };

    // First pass: collect all valid pairs and find the minimum non-self distance
    let mut pairs = Vec::new();
    let mut min_distance = u32::MAX;

    for i in 0..num_cpu_groups {
        for j in (i + 1)..num_cpu_groups {
            if let Some(dist) = get_distance(i, j)
                && dist > 0
            {
                min_distance = min_distance.min(dist);
                pairs.push((i, j, dist));
            }
        }
    }

    if pairs.is_empty() || min_distance == u32::MAX || min_distance == 0 {
        return ResourceDescriptorCoupling::default();
    }

    // Second pass: compute weights using Option A (Normalized Latency Inversion)
    // W(D) = round(256 * min_dist / D), clamped to [1, 256]
    let mut weights = Vec::with_capacity(pairs.len());
    for (i, j, dist) in pairs {
        let weight = (((256u64 * min_distance as u64) as f64 / dist as f64).round() as u64)
            .clamp(1, 256) as u16;
        let mut item = ResourceDescriptorCouplingItem {
            resource1_idx: cpu_resource_idx,
            group1_idx: (i as u8).into(),
            resource2_idx: cpu_resource_idx,
            group2_idx: (j as u8).into(),
            weight,
        };
        item.normalize();
        weights.push(item);
    }

    weights.sort_unstable();
    ResourceDescriptorCoupling { weights }
}

/// Automatically derives NUMA distance coupling for the `cpus` resource in `resources`.
pub fn detect_numa_coupling(
    resources: &[ResourceDescriptorItem],
    topology: &NumaTopology,
) -> ResourceDescriptorCoupling {
    let Some((cpu_idx, cpu_res)) = resources
        .iter()
        .enumerate()
        .find(|(_, r)| r.name == CPU_RESOURCE_NAME)
    else {
        return ResourceDescriptorCoupling::default();
    };

    let n_groups = cpu_res.kind.n_groups();
    if n_groups <= 1 {
        return ResourceDescriptorCoupling::default();
    }

    numa_distances_to_coupling(topology, cpu_idx as u8, n_groups)
}

/// Merges user-specified coupling on top of a base coupling.
/// User-specified weights override matching base weights, and new items are added.
pub fn merge_coupling(
    base: ResourceDescriptorCoupling,
    override_coupling: ResourceDescriptorCoupling,
) -> ResourceDescriptorCoupling {
    let mut weights = base.weights;
    for mut item in override_coupling.weights {
        item.normalize();
        if let Some(existing) = weights.iter_mut().find(|w| {
            w.resource1_idx == item.resource1_idx
                && w.group1_idx == item.group1_idx
                && w.resource2_idx == item.resource2_idx
                && w.group2_idx == item.group2_idx
        }) {
            existing.weight = item.weight;
        } else {
            weights.push(item);
        }
    }
    weights.sort_unstable();
    ResourceDescriptorCoupling { weights }
}

/// Detects the NUMA topology and attached accelerators on the current machine.
/// Uses zero external dependencies by directly querying Linux sysfs and procfs.
pub fn detect_numa_topology() -> NumaTopology {
    #[cfg(target_os = "linux")]
    {
        NumaTopology {
            nodes: detect_numa_nodes(),
            gpus: detect_gpus(),
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        NumaTopology::default()
    }
}

#[cfg(target_os = "linux")]
pub fn detect_numa_nodes() -> Vec<NumaNodeInfo> {
    if let Ok(mock_val) = std::env::var("HQ_MOCK_NUMA_NODES")
        && let Ok(n_nodes) = mock_val.parse::<u32>()
        && n_nodes > 1
    {
        return generate_mock_numa_nodes(n_nodes);
    }

    let node_dir = Path::new("/sys/devices/system/node");
    let mut nodes = Vec::new();

    if node_dir.is_dir()
        && let Ok(entries) = fs::read_dir(node_dir)
    {
        for entry in entries.flatten() {
            let file_name = entry.file_name();
            let name = file_name.to_string_lossy();
            if let Some(stripped) = name.strip_prefix("node")
                && let Ok(node_id) = stripped.parse::<u32>()
            {
                let path = entry.path();
                let cpulist =
                    read_trimmed(path.join("cpulist")).unwrap_or_else(|| "N/A".to_string());
                let cpu_count = count_cpus_in_list(&cpulist);

                let (mem_total, mem_free) = parse_node_meminfo(&path.join("meminfo"));
                let distances = parse_node_distances(&path.join("distance"));

                nodes.push(NumaNodeInfo {
                    id: node_id,
                    cpulist,
                    cpu_count,
                    mem_total_bytes: mem_total.map(|kb| kb * 1024),
                    mem_free_bytes: mem_free.map(|kb| kb * 1024),
                    distances,
                });
            }
        }
    }

    nodes.sort_by_key(|n| n.id);

    // Fallback: If no /sys/devices/system/node or no nodeX subdirectories, check UMA
    if nodes.is_empty()
        && let Some(cpulist) = read_trimmed("/sys/devices/system/cpu/online")
            .or_else(|| read_trimmed("/sys/devices/system/cpu/present"))
    {
        let cpu_count = count_cpus_in_list(&cpulist);
        let (mem_total, mem_free) = parse_proc_meminfo();
        nodes.push(NumaNodeInfo {
            id: 0,
            cpulist,
            cpu_count,
            mem_total_bytes: mem_total.map(|kb| kb * 1024),
            mem_free_bytes: mem_free.map(|kb| kb * 1024),
            distances: vec![10],
        });
    }

    nodes
}

#[cfg(target_os = "linux")]
fn generate_mock_numa_nodes(n_nodes: u32) -> Vec<NumaNodeInfo> {
    let total_cpus = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(8);
    let cpus_per_node = total_cpus.div_ceil(n_nodes as usize);
    let (mem_total, mem_free) = parse_proc_meminfo();
    let node_mem_total = mem_total.map(|m| (m / (n_nodes as u64)) * 1024);
    let node_mem_free = mem_free.map(|m| (m / (n_nodes as u64)) * 1024);

    let mut nodes = Vec::new();
    for i in 0..n_nodes {
        let start = (i as usize) * cpus_per_node;
        let end = (start + cpus_per_node).min(total_cpus);
        let cpulist = if start < end {
            if end - start == 1 {
                format!("{start}")
            } else {
                format!("{start}-{}", end - 1)
            }
        } else {
            String::new()
        };
        let cpu_count = end.saturating_sub(start);

        let distances = (0..n_nodes)
            .map(|j| if i == j { 10 } else { 20 })
            .collect();

        nodes.push(NumaNodeInfo {
            id: i,
            cpulist,
            cpu_count,
            mem_total_bytes: node_mem_total,
            mem_free_bytes: node_mem_free,
            distances,
        });
    }
    nodes
}

#[cfg(target_os = "linux")]
pub fn detect_gpus() -> Vec<GpuPciInfo> {
    let pci_dir = Path::new("/sys/bus/pci/devices");
    let mut gpus = Vec::new();

    if !pci_dir.is_dir() {
        return gpus;
    }

    let entries = match fs::read_dir(pci_dir) {
        Ok(e) => e,
        Err(_) => return gpus,
    };

    for entry in entries.flatten() {
        let pci_address = entry.file_name().to_string_lossy().to_string();
        let path = entry.path();

        let class_id = match read_hex_u32(path.join("class")) {
            Some(c) => c,
            None => continue,
        };

        let vendor_id = match read_hex_u16(path.join("vendor")) {
            Some(v) => v,
            None => continue,
        };

        let device_id = match read_hex_u16(path.join("device")) {
            Some(d) => d,
            None => continue,
        };

        // Class checks:
        // Base class 0x03: Display Controller (VGA=0x0300, 3D Controller=0x0302 like NVIDIA Tesla/A100/H100, Other=0x0380)
        // Base class 0x12: Processing Accelerators (HPC accelerators like Google TPU, Intel Gaudi)
        let base_class = (class_id >> 16) as u8;
        let is_display_or_3d = base_class == 0x03;
        let is_accelerator = base_class == 0x12;

        // Only accept Display/3D controllers or Processing Accelerators.
        // This filters out audio controllers (PCI class 0x0403), USB controllers (0x0c03),
        // and other non-GPU companion functions on multi-function graphics cards.
        if !is_display_or_3d && !is_accelerator {
            continue;
        }

        let vendor_name = match vendor_id {
            0x10de => "NVIDIA Corporation".to_string(),
            0x1002 => "Advanced Micro Devices, Inc. [AMD/ATI]".to_string(),
            0x8086 => "Intel Corporation".to_string(),
            0x1da3 => "Habana Labs Ltd. (Intel)".to_string(),
            0x1ae0 => "Google, Inc.".to_string(),
            other => format!("Vendor 0x{:04x}", other),
        };

        let device_name = lookup_pci_device_name(vendor_id, device_id);

        let driver = fs::read_link(path.join("driver"))
            .ok()
            .and_then(|p| p.file_name().map(|n| n.to_string_lossy().to_string()));

        let numa_node = read_trimmed(path.join("numa_node")).and_then(|s| s.parse::<i32>().ok());

        let local_cpulist = read_trimmed(path.join("local_cpulist"));

        gpus.push(GpuPciInfo {
            pci_address,
            vendor_id,
            device_id,
            class_id,
            vendor_name,
            device_name,
            driver,
            numa_node,
            local_cpulist,
        });
    }

    gpus.sort_by(|a, b| a.pci_address.cmp(&b.pci_address));
    gpus
}

/// Reads a file and trims leading/trailing whitespace.
fn read_trimmed<P: AsRef<Path>>(path: P) -> Option<String> {
    fs::read_to_string(path).ok().map(|s| s.trim().to_string())
}

/// Reads a hex string (e.g. "0x030000" or "30000") into a u32.
fn read_hex_u32<P: AsRef<Path>>(path: P) -> Option<u32> {
    let s = read_trimmed(path)?;
    let clean = s.trim_start_matches("0x");
    u32::from_str_radix(clean, 16).ok()
}

/// Reads a hex string (e.g. "0x10de") into a u16.
fn read_hex_u16<P: AsRef<Path>>(path: P) -> Option<u16> {
    let s = read_trimmed(path)?;
    let clean = s.trim_start_matches("0x");
    u16::from_str_radix(clean, 16).ok()
}

/// Counts total CPUs in a Linux cpulist string like "0-3,5,8-10".
pub fn count_cpus_in_list(list: &str) -> usize {
    if list.trim().is_empty() || list == "N/A" {
        return 0;
    }
    let mut total = 0;
    for part in list.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((start_s, end_s)) = part.split_once('-') {
            if let (Ok(start), Ok(end)) = (start_s.parse::<usize>(), end_s.parse::<usize>())
                && end >= start
            {
                total += (end - start) + 1;
            }
        } else if part.parse::<usize>().is_ok() {
            total += 1;
        }
    }
    total
}

/// Parses per-node `/sys/devices/system/node/nodeX/meminfo`
fn parse_node_meminfo(path: &Path) -> (Option<u64>, Option<u64>) {
    let file = match fs::File::open(path) {
        Ok(f) => f,
        Err(_) => return (None, None),
    };
    let reader = io::BufReader::new(file);

    let mut mem_total = None;
    let mut mem_free = None;

    for line in reader.lines().map_while(Result::ok) {
        if line.contains("MemTotal:") {
            mem_total = extract_kb_from_line(&line);
        } else if line.contains("MemFree:") {
            mem_free = extract_kb_from_line(&line);
        }
    }

    (mem_total, mem_free)
}

/// Parses system-wide `/proc/meminfo`
#[cfg(target_os = "linux")]
fn parse_proc_meminfo() -> (Option<u64>, Option<u64>) {
    let file = match fs::File::open("/proc/meminfo") {
        Ok(f) => f,
        Err(_) => return (None, None),
    };
    let reader = io::BufReader::new(file);

    let mut total = None;
    let mut free = None;
    for line in reader.lines().map_while(Result::ok) {
        if line.starts_with("MemTotal:") {
            total = extract_kb_from_line(&line);
        } else if line.starts_with("MemAvailable:")
            || (free.is_none() && line.starts_with("MemFree:"))
        {
            free = extract_kb_from_line(&line);
        }
    }
    (total, free)
}

fn extract_kb_from_line(line: &str) -> Option<u64> {
    line.split_whitespace()
        .rev()
        .nth(1)
        .and_then(|val| val.parse::<u64>().ok())
}

/// Parses `/sys/devices/system/node/nodeX/distance`
fn parse_node_distances(path: &Path) -> Vec<u32> {
    if let Some(content) = read_trimmed(path) {
        content
            .split_whitespace()
            .filter_map(|s| s.parse::<u32>().ok())
            .collect()
    } else {
        Vec::new()
    }
}

/// Attempts to look up the PCI device model name from system `pci.ids`.
/// Common paths: `/usr/share/hwdata/pci.ids` (Fedora/RHEL/Rocky/Arch) or `/usr/share/misc/pci.ids` (Debian/Ubuntu).
fn lookup_pci_device_name(vendor_id: u16, device_id: u16) -> Option<String> {
    let candidate_paths = [
        "/usr/share/hwdata/pci.ids",
        "/usr/share/misc/pci.ids",
        "/var/lib/pciutils/pci.ids",
    ];

    let pci_ids_path = candidate_paths.iter().find(|p| Path::new(p).exists())?;
    let file = fs::File::open(pci_ids_path).ok()?;
    let reader = io::BufReader::new(file);

    let vendor_hex = format!("{:04x}", vendor_id);
    let device_hex = format!("{:04x}", device_id);

    let mut inside_target_vendor = false;

    for line in reader.lines().map_while(Result::ok) {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }

        if !line.starts_with('\t') {
            let v_id = line.split_whitespace().next().unwrap_or("");
            inside_target_vendor = v_id.eq_ignore_ascii_case(&vendor_hex);
        } else if inside_target_vendor && line.starts_with('\t') && !line.starts_with("\t\t") {
            let mut parts = line[1..].splitn(2, "  ");
            if let Some(d_id) = parts.next()
                && d_id.trim().eq_ignore_ascii_case(&device_hex)
                && let Some(name) = parts.next()
            {
                return Some(name.trim().to_string());
            }
        } else if inside_target_vendor && !line.starts_with('\t') {
            break;
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_cpus_in_list() {
        assert_eq!(count_cpus_in_list("0-3"), 4);
        assert_eq!(count_cpus_in_list("0-3,5"), 5);
        assert_eq!(count_cpus_in_list("0-3,5,8-10"), 8);
        assert_eq!(count_cpus_in_list("10"), 1);
        assert_eq!(count_cpus_in_list(""), 0);
        assert_eq!(count_cpus_in_list("N/A"), 0);
    }

    #[test]
    fn test_numa_topology_methods() {
        let topo = NumaTopology {
            nodes: vec![
                NumaNodeInfo {
                    id: 0,
                    cpulist: "0-7".to_string(),
                    cpu_count: 8,
                    mem_total_bytes: Some(1024 * 1024 * 1024),
                    mem_free_bytes: Some(512 * 1024 * 1024),
                    distances: vec![10, 20],
                },
                NumaNodeInfo {
                    id: 1,
                    cpulist: "8-15".to_string(),
                    cpu_count: 8,
                    mem_total_bytes: Some(1024 * 1024 * 1024),
                    mem_free_bytes: Some(512 * 1024 * 1024),
                    distances: vec![20, 10],
                },
            ],
            gpus: vec![GpuPciInfo {
                pci_address: "0000:01:00.0".to_string(),
                vendor_id: 0x10de,
                device_id: 0x1fdd,
                class_id: 0x030000,
                vendor_name: "NVIDIA Corporation".to_string(),
                device_name: Some("GeForce GTX 1650".to_string()),
                driver: Some("nvidia".to_string()),
                numa_node: Some(0),
                local_cpulist: Some("0-7".to_string()),
            }],
        };

        assert_eq!(topo.total_cpus(), 16);
        assert_eq!(topo.total_memory_bytes(), Some(2 * 1024 * 1024 * 1024));
        assert_eq!(topo.gpus_for_node(0).len(), 1);
        assert_eq!(topo.gpus_for_node(1).len(), 0);
    }

    #[test]
    fn test_numa_distances_to_coupling_8_nodes_user_matrix() {
        let matrix = [
            vec![10, 12, 12, 12, 32, 32, 32, 32],
            vec![12, 10, 12, 12, 32, 32, 32, 32],
            vec![12, 12, 10, 12, 32, 32, 32, 32],
            vec![12, 12, 12, 10, 32, 32, 32, 32],
            vec![32, 32, 32, 32, 10, 12, 12, 12],
            vec![32, 32, 32, 32, 12, 10, 12, 12],
            vec![32, 32, 32, 32, 12, 12, 10, 12],
            vec![32, 32, 32, 32, 12, 12, 12, 10],
        ];

        let nodes = (0..8)
            .map(|id| NumaNodeInfo {
                id,
                cpulist: format!("{}-{}", id * 10, id * 10 + 9),
                cpu_count: 10,
                mem_total_bytes: Some(16 * 1024 * 1024 * 1024),
                mem_free_bytes: Some(8 * 1024 * 1024 * 1024),
                distances: matrix[id as usize].clone(),
            })
            .collect();

        let topo = NumaTopology {
            nodes,
            gpus: vec![],
        };

        let coupling = numa_distances_to_coupling(&topo, 0, 8);
        assert_eq!(coupling.weights.len(), 28); // 8 choose 2

        // Verify intra-socket weights (dist 12 -> 256)
        // Socket 0 intra-pairs
        for i in 0..4u8 {
            for j in (i + 1)..4u8 {
                let item = coupling
                    .weights
                    .iter()
                    .find(|w| w.group1_idx == i.into() && w.group2_idx == j.into())
                    .expect("Intra-socket S0 edge missing");
                assert_eq!(item.weight, 256);
            }
        }
        // Socket 1 intra-pairs
        for i in 4..8u8 {
            for j in (i + 1)..8u8 {
                let item = coupling
                    .weights
                    .iter()
                    .find(|w| w.group1_idx == i.into() && w.group2_idx == j.into())
                    .expect("Intra-socket S1 edge missing");
                assert_eq!(item.weight, 256);
            }
        }

        // Verify inter-socket weights (dist 32 -> round(256 * 12 / 32) = 96)
        for i in 0..4u8 {
            for j in 4..8u8 {
                let item = coupling
                    .weights
                    .iter()
                    .find(|w| w.group1_idx == i.into() && w.group2_idx == j.into())
                    .expect("Inter-socket edge missing");
                assert_eq!(item.weight, 96);
            }
        }
    }

    #[test]
    fn test_numa_distances_to_coupling_variable_sizes() {
        // 2-node dual-socket system
        let topo_2 = NumaTopology {
            nodes: vec![
                NumaNodeInfo {
                    id: 0,
                    cpulist: "0-3".to_string(),
                    cpu_count: 4,
                    distances: vec![10, 20],
                    ..Default::default()
                },
                NumaNodeInfo {
                    id: 1,
                    cpulist: "4-7".to_string(),
                    cpu_count: 4,
                    distances: vec![20, 10],
                    ..Default::default()
                },
            ],
            gpus: vec![],
        };
        let c2 = numa_distances_to_coupling(&topo_2, 0, 2);
        assert_eq!(c2.weights.len(), 1);
        assert_eq!(c2.weights[0].weight, 256);

        // 4-node system with 3 distance tiers (10 self, 16 near, 24 far)
        let topo_4 = NumaTopology {
            nodes: vec![
                NumaNodeInfo {
                    id: 0,
                    cpulist: "0-3".to_string(),
                    cpu_count: 4,
                    distances: vec![10, 16, 24, 16],
                    ..Default::default()
                },
                NumaNodeInfo {
                    id: 1,
                    cpulist: "4-7".to_string(),
                    cpu_count: 4,
                    distances: vec![16, 10, 16, 24],
                    ..Default::default()
                },
                NumaNodeInfo {
                    id: 2,
                    cpulist: "8-11".to_string(),
                    cpu_count: 4,
                    distances: vec![24, 16, 10, 16],
                    ..Default::default()
                },
                NumaNodeInfo {
                    id: 3,
                    cpulist: "12-15".to_string(),
                    cpu_count: 4,
                    distances: vec![16, 24, 16, 10],
                    ..Default::default()
                },
            ],
            gpus: vec![],
        };
        let c4 = numa_distances_to_coupling(&topo_4, 0, 4);
        assert_eq!(c4.weights.len(), 6); // 4 choose 2
        // Dist 16 -> 256, Dist 24 -> round(256 * 16 / 24) = 171
        let near = c4
            .weights
            .iter()
            .find(|w| w.group1_idx == 0.into() && w.group2_idx == 1.into())
            .unwrap();
        assert_eq!(near.weight, 256);
        let far = c4
            .weights
            .iter()
            .find(|w| w.group1_idx == 0.into() && w.group2_idx == 2.into())
            .unwrap();
        assert_eq!(far.weight, 171);

        // Single node -> no coupling
        let topo_1 = NumaTopology {
            nodes: vec![NumaNodeInfo {
                id: 0,
                cpulist: "0-15".to_string(),
                cpu_count: 16,
                distances: vec![10],
                ..Default::default()
            }],
            gpus: vec![],
        };
        let c1 = numa_distances_to_coupling(&topo_1, 0, 1);
        assert!(c1.weights.is_empty());
    }

    #[test]
    fn test_merge_coupling() {
        use tako::resources::ResourceDescriptorCouplingItem;

        let base = ResourceDescriptorCoupling {
            weights: vec![
                ResourceDescriptorCouplingItem {
                    resource1_idx: 0,
                    group1_idx: 0.into(),
                    resource2_idx: 0,
                    group2_idx: 1.into(),
                    weight: 256,
                },
                ResourceDescriptorCouplingItem {
                    resource1_idx: 0,
                    group1_idx: 0.into(),
                    resource2_idx: 0,
                    group2_idx: 2.into(),
                    weight: 96,
                },
            ],
        };

        // User overrides (0, 0) <-> (0, 2) to 150, and adds GPU (0, 0) <-> (1, 0) with weight 256
        let user = ResourceDescriptorCoupling {
            weights: vec![
                ResourceDescriptorCouplingItem {
                    resource1_idx: 0,
                    group1_idx: 2.into(),
                    resource2_idx: 0,
                    group2_idx: 0.into(),
                    weight: 150,
                },
                ResourceDescriptorCouplingItem {
                    resource1_idx: 0,
                    group1_idx: 0.into(),
                    resource2_idx: 1,
                    group2_idx: 0.into(),
                    weight: 256,
                },
            ],
        };

        let merged = merge_coupling(base, user);
        assert_eq!(merged.weights.len(), 3);
        assert_eq!(
            merged
                .weights
                .iter()
                .find(|w| w.resource1_idx == 0
                    && w.group1_idx == 0.into()
                    && w.resource2_idx == 0
                    && w.group2_idx == 1.into())
                .unwrap()
                .weight,
            256
        );
        assert_eq!(
            merged
                .weights
                .iter()
                .find(|w| w.resource1_idx == 0
                    && w.group1_idx == 0.into()
                    && w.resource2_idx == 0
                    && w.group2_idx == 2.into())
                .unwrap()
                .weight,
            150
        );
        assert_eq!(
            merged
                .weights
                .iter()
                .find(|w| w.resource1_idx == 0
                    && w.group1_idx == 0.into()
                    && w.resource2_idx == 1
                    && w.group2_idx == 0.into())
                .unwrap()
                .weight,
            256
        );
    }
}
