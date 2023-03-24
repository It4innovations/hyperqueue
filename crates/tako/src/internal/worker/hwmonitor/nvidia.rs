use crate::hwstats::{GpuCollectionStats, GpuStats};
use crate::internal::common::error::DsError;
use std::process::Command;

/// Parses compute and memory utilization of Nvidia GPUs using `nvidia-smi`.
/// Example expected output:
/// ```console
/// $ nvidia-smi --format=csv,noheader --query-gpu=pci.bus_id,utilization.gpu,memory.used,memory.total
/// 00000000:C8:00.0, 0 %, 0 MiB, 6144 MiB
/// 00000000:CB:00.0, 0 %, 0 MiB, 6144 MiB
/// ```
pub fn get_nvidia_gpu_state() -> crate::Result<GpuCollectionStats> {
    let mut command = Command::new("nvidia-smi");
    command.args([
        "--format=csv,noheader",
        "--query-gpu=pci.bus_id,utilization.gpu,memory.used,memory.total",
    ]);
    let output = command
        .output()
        .map_err::<DsError, _>(|error| format!("Cannot execute nvidia-smi: {error:?}").into())?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !output.status.success() {
        return Err(format!(
            "nvidia-smi exited with error code {}\nStdout: {stdout}\nStderr: {stderr}",
            output.status
        )
        .into());
    }

    parse_nvidia_gpu_stats(&stdout)
}

fn parse_nvidia_gpu_stats(output: &str) -> crate::Result<GpuCollectionStats> {
    let mut gpus = Vec::new();
    for line in output.lines() {
        let mut iter = line.split(',').map(|v| v.trim());
        let bus_id = iter.next().unwrap_or("");
        let gpu_util = iter
            .next()
            .unwrap_or("")
            .trim_end_matches('%')
            .trim()
            .parse::<f32>()
            .unwrap_or(0.0);
        let memory_used = iter.next().and_then(parse_nvidia_gpu_memory).unwrap_or(0.0);
        let memory_total = iter.next().and_then(parse_nvidia_gpu_memory).unwrap_or(0.0);

        let memory_usage = if memory_total > 0.0 {
            (memory_used / memory_total) * 100.0
        } else {
            0.0
        };
        gpus.push(GpuStats {
            id: bus_id.to_string(),
            processor_usage: gpu_util,
            mem_usage: memory_usage,
        });
    }

    Ok(GpuCollectionStats { gpus })
}

fn parse_nvidia_gpu_memory(input: &str) -> Option<f32> {
    let mut iter = input.split(' ');
    let value = iter.next().and_then(|v| v.parse::<f32>().ok())?;
    let suffix = iter.next().unwrap_or("").trim();
    let multiplier = match suffix {
        "KiB" => 1024,
        "MiB" => 1024 * 1024,
        "GiB" => 1024 * 1024 * 1024,
        _ => 1,
    } as f32;
    Some(value * multiplier)
}

#[cfg(test)]
mod tests {
    use crate::hwstats::GpuStats;
    use crate::internal::worker::hwmonitor::nvidia::{
        parse_nvidia_gpu_memory, parse_nvidia_gpu_stats,
    };

    #[test]
    fn test_parse_nvidia_gpu_memory() {
        assert_eq!(parse_nvidia_gpu_memory("123.5").unwrap(), 123.5);
        assert_eq!(parse_nvidia_gpu_memory("123.2 KiB").unwrap(), 126156.8);
        assert_eq!(parse_nvidia_gpu_memory("123.2 MiB").unwrap(), 129184560.0);
        assert_eq!(
            parse_nvidia_gpu_memory("123.4 GiB").unwrap(),
            132499740000.0
        );
    }

    #[test]
    fn test_parse_nvidia_gpu_stats() {
        let mut stats = parse_nvidia_gpu_stats("BUS1, 5.2 %, 100 MiB, 200 MiB").unwrap();
        assert_eq!(stats.gpus.len(), 1);

        let gpu = stats.gpus.pop().unwrap();
        let GpuStats {
            id,
            processor_usage: cpu_usage,
            mem_usage,
        } = gpu;
        assert_eq!(id, "BUS1");
        assert_eq!(cpu_usage, 5.2);
        assert_eq!(mem_usage, 50.0);
    }
}
