use tako::common::resources::{CpuId, NumOfCpus, ResourceDescriptor};

pub fn parse_range(line: &str) -> anyhow::Result<Vec<CpuId>> {
    let line = line.trim();
    let mut result = Vec::new();
    for part in line.split(',') {
        let part = part.trim();
        if part.contains('-') {
            let ids: Vec<&str> = part.split('-').collect();
            if ids.len() != 2 {
                anyhow::bail!("Invalid format of range");
            }
            for i in ids[0].parse()?..=ids[1].parse()? {
                result.push(i);
            }
        } else {
            let id: CpuId = part.parse()?;
            result.push(id);
        }
    }
    Ok(result)
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
    }

    #[test]
    fn test_read_linux_numa() {
        let cpus = read_linux_numa().unwrap();
        assert_eq!(cpus.iter().map(|x| x.len()).sum::<usize>(), num_cpus::get());
    }
}
