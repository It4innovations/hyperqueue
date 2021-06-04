use tako::common::resources::{NumOfCpus, ResourceDescriptor};

pub fn detect_resource() -> anyhow::Result<ResourceDescriptor> {
    let n_cpus = num_cpus::get() as NumOfCpus;
    if n_cpus < 1 {
        anyhow::bail!("Cpu detection failed".to_string());
    };
    Ok(ResourceDescriptor::new_with_socket_size(1, n_cpus))
}
