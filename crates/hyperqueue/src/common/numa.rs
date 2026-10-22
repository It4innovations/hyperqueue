use std::fs;
use std::io::{self, BufRead};
use std::path::Path;

use serde::{Deserialize, Serialize};
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

                let (mem_total, mem_free, _) = parse_node_meminfo(&path.join("meminfo"));
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
        let is_known_gpu_vendor = vendor_id == 0x10de // NVIDIA
            || vendor_id == 0x1002 // AMD
            || vendor_id == 0x1da3; // Habana / Gaudi

        if !is_display_or_3d && !is_accelerator && !is_known_gpu_vendor {
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
fn parse_node_meminfo(path: &Path) -> (Option<u64>, Option<u64>, Option<u64>) {
    let file = match fs::File::open(path) {
        Ok(f) => f,
        Err(_) => return (None, None, None),
    };
    let reader = io::BufReader::new(file);

    let mut mem_total = None;
    let mut mem_free = None;
    let mut mem_used = None;

    for line in reader.lines().map_while(Result::ok) {
        if line.contains("MemTotal:") {
            mem_total = extract_kb_from_line(&line);
        } else if line.contains("MemFree:") {
            mem_free = extract_kb_from_line(&line);
        } else if line.contains("MemUsed:") {
            mem_used = extract_kb_from_line(&line);
        }
    }

    if mem_used.is_none()
        && let (Some(t), Some(f)) = (mem_total, mem_free)
    {
        mem_used = Some(t.saturating_sub(f));
    }

    (mem_total, mem_free, mem_used)
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
        .nth(line.split_whitespace().count().saturating_sub(2))
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
}
