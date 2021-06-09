use tako::common::resources::{CpuId, NumOfCpus, ResourceDescriptor};

pub fn parse_range(line: &str) -> anyhow::Result<(CpuId, CpuId)> {
    let line = line.trim();
    if line.contains('-') {
        let ids: Vec<&str> = line.split('-').collect();
        if ids.len() != 2 {
            anyhow::bail!("Invalid format of range");
        }
        Ok((ids[0].parse()?, ids[1].parse()?))
    } else {
        let id: CpuId = line.parse()?;
        Ok((id, id))
    }
}

pub fn read_linux_numa() -> anyhow::Result<Vec<Vec<CpuId>>> {
    let nodes = parse_range(&std::fs::read_to_string(
        "/sys/devices/system/node/possible",
    )?)?;
    let mut cpus: Vec<Vec<CpuId>> = Vec::new();
    for numa_index in nodes.0..=nodes.1 {
        let filename = format!("/sys/devices/system/node/node{}/cpulist", numa_index);
        let cpu_range = parse_range(&std::fs::read_to_string(filename)?)?;
        cpus.push((cpu_range.0..=cpu_range.1).collect());
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
    use super::parse_range;
    use crate::worker::hwdetect::read_linux_numa;

    #[test]
    fn test_parse_range() {
        assert!(matches!(parse_range("10"), Ok((10, 10))));
        assert!(matches!(parse_range("10\n"), Ok((10, 10))));
        assert!(matches!(parse_range("0-3\n"), Ok((0, 3))));
        assert!(matches!(parse_range("111-12388\n"), Ok((111, 12388))));
        assert!(parse_range("xx\n").is_err());
        assert!(parse_range("-\n").is_err());
        assert!(parse_range("-2\n").is_err());
        assert!(parse_range("0-1-2\n").is_err());
    }

    #[test]
    fn test_read_linux_numa() {
        let cpus = read_linux_numa().unwrap();
        assert_eq!(cpus.iter().map(|x| x.len()).sum::<usize>(), num_cpus::get());
    }
}
