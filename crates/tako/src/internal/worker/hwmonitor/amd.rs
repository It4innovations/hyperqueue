use crate::hwstats::{GpuCollectionStats, GpuStats};
use crate::internal::common::error::DsError;
use crate::Map;
use std::process::Command;

/// Parses compute and memory utilization of AMD GPUs using `rocm-smi`.
/// Example expected output:
/// ```json
/// {
///     "card0": {"GPU use (%)": "0", "GPU memory use (%)": "0", "PCI Bus": "0000:C1:00.0"},
///     "card1": {"GPU use (%)": "0", "GPU memory use (%)": "0", "PCI Bus": "0000:C6:00.0"}
/// }
/// ```
pub fn get_amd_gpu_state() -> crate::Result<GpuCollectionStats> {
    let mut command = Command::new("rocm-smi");
    command.args(["--json", "--showuse", "--showbus", "--showmemuse"]);
    let output = command
        .output()
        .map_err::<DsError, _>(|error| format!("Cannot execute rocm-smi: {error:?}").into())?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !output.status.success() {
        return Err(format!(
            "rocm-smi exited with error code {}\nStdout: {stdout}\nStderr: {stderr}",
            output.status
        )
        .into());
    }

    parse_amd_gpu_stats(&stdout)
}

#[derive(serde::Deserialize)]
struct AmdGpuStats {
    #[serde(rename = "GPU use (%)")]
    pub gpu_use: String,
    #[serde(rename = "GPU memory use (%)")]
    pub gpu_memory_use: String,
    #[serde(rename = "PCI Bus")]
    pub pci_bus: String,
}

fn parse_amd_gpu_stats(output: &str) -> crate::Result<GpuCollectionStats> {
    let gpus: Map<String, AmdGpuStats> = serde_json::from_str(output).map_err(|error| {
        DsError::SerializationError(format!(
            "Cannot deserialize AMD GPU state from `rocm-smi`: {error:?}"
        ))
    })?;
    let mut gpus: Vec<(String, AmdGpuStats)> = gpus.into_iter().collect();
    gpus.sort_by(|(name_a, _), (name_b, _)| name_a.cmp(name_b));

    Ok(GpuCollectionStats {
        gpus: gpus
            .into_iter()
            .map(|(_, gpu)| GpuStats {
                id: gpu.pci_bus,
                processor_usage: gpu.gpu_use.parse::<f32>().unwrap_or(0.0),
                mem_usage: gpu.gpu_memory_use.parse::<f32>().unwrap_or(0.0),
            })
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use crate::internal::worker::hwmonitor::amd::parse_amd_gpu_stats;

    #[test]
    fn test_parse_amd_gpu_stats() {
        let gpus = parse_amd_gpu_stats(r#"{"card0": {"GPU use (%)": "5.2", "GFX Activity": "2497926857", "GPU memory use (%)": "14.8", "Memory Activity": "360832048", "PCI Bus": "0000:C1:00.0", "Card series": "AMD INSTINCT MI200 (MCM) OAM LC MBA HPE C2", "Card model": "FirePro W4300", "Card vendor": "Advanced Micro Devices, Inc. [AMD/ATI]", "Card SKU": "D65201"}, "card1": {"GPU use (%)": "0", "GFX Activity": "819691187", "GPU memory use (%)": "0", "Memory Activity": "172637159", "PCI Bus": "0000:C6:00.0", "Card series": "AMD INSTINCT MI200 (MCM) OAM LC MBA HPE C2", "Card model": "FirePro W4300", "Card vendor": "Advanced Micro Devices, Inc. [AMD/ATI]", "Card SKU": "D65201"}}"#).unwrap().gpus;
        assert_eq!(gpus.len(), 2);
        assert_eq!(gpus[0].id, "0000:C1:00.0");
        assert_eq!(gpus[0].processor_usage, 5.2);
        assert_eq!(gpus[0].mem_usage, 14.8);
        assert_eq!(gpus[1].id, "0000:C6:00.0");
        assert_eq!(gpus[1].processor_usage, 0.0);
        assert_eq!(gpus[1].mem_usage, 0.0);
    }
}
