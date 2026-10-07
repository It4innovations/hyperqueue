"""Register Nibi queues with the real CLI, without jobs, workers, or SLURM submissions."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from test_journal_directory import run, server

ROOT = Path(__file__).resolve().parents[1]
# CPUs, memory MiB, class, total cap, backlog, exclusive, GPU type
EXPECTED = {
    "cpu_base_full": (192, 766000, "worker/cpu", 100, 25, True, None),
    "cpu_base_half": (96, 383000, "worker/cpu", 1, 1, False, None),
    "cpu_base_quarter": (48, 191500, "worker/cpu", 1, 1, False, None),
    "cpu_base_eighth": (24, 95750, "worker/cpu", 1, 1, False, None),
    "cpu_base_sixteenth": (12, 47875, "worker/cpu", 1, 1, False, None),
    "cpu_large_full": (192, 6144000, "worker/cpuLarge", 4, 2, True, None),
    "cpu_large_half": (96, 3072000, "worker/cpuLarge", 1, 1, False, None),
    "cpu_large_quarter": (48, 1536000, "worker/cpuLarge", 1, 1, False, None),
    "mi300a": (24, 126750, "worker/mi300a", 8, 4, False, "mi300a"),
    "h100_full": (14, 256000, "worker/h100", 16, 8, False, "nvidia_h100_80gb_hbm3"),
    "h100_1g.10gb": (2, 31744, "worker/h100mig10", 48, 24, False, "nvidia_h100_80gb_hbm3_1g.10gb"),
    "h100_2g.20gb": (4, 63488, "worker/h100mig20", 24, 12, False, "nvidia_h100_80gb_hbm3_2g.20gb"),
    "H100-3g.40gb": (6, 126976, "worker/h100mig40", 24, 12, False, "nvidia_h100_80gb_hbm3_3g.40gb"),
}


def main():
    binary = Path(sys.argv[1]).resolve()
    with tempfile.TemporaryDirectory(prefix="hq-nibi-") as tmp:
        root = Path(tmp)
        env = dict(os.environ, HQ_SERVER_DIR=str(root / "server"), SLURM_ACCOUNT="def-test")
        commands = root / "commands.jsonl"
        wrapper = root / "bin" / "hq"
        wrapper.parent.mkdir()
        wrapper.write_text(
            "#!/usr/bin/env python3\nimport json,os,sys\n"
            "with open(os.environ['HQ_TEST_COMMANDS'],'a') as f: f.write(json.dumps(sys.argv[1:])+'\\n')\n"
            "os.execv(os.environ['HQ_TEST_BINARY'], [os.environ['HQ_TEST_BINARY'], *sys.argv[1:]])\n"
        )
        wrapper.chmod(0o755)
        env.update(HQ_TEST_COMMANDS=str(commands), HQ_TEST_BINARY=str(binary))
        env["PATH"] = str(wrapper.parent) + os.pathsep + env["PATH"]
        with server(binary, root, env, "--no-journal"):
            subprocess.run(
                ["bash", str(ROOT / "configs/nibi.sh")],
                cwd=root,
                env=env,
                check=True,
                capture_output=True,
                text=True,
                timeout=60,
            )
            queues = json.loads(run(binary, root, env, "--output-mode=json", "alloc", "list").stdout)
            assert len(queues) == 65
            submitted = [json.loads(line) for line in commands.read_text().splitlines()]
            assert len(submitted) == 65
            for args in submitted:
                name = args[args.index("--name") + 1].rsplit("-", 1)[0]
                assert args[args.index("--idle-timeout") + 1] == "5m"
                assert "--no-dry-run" in args
                if name.startswith("cpu_") and name != "cpu_base_sixteenth":
                    assert args[args.index("--allocation-min-utilization") + 1] == "0.5"
                else:
                    assert "--allocation-min-utilization" not in args
            for queue in queues:
                name, hours = queue["name"].rsplit("-", 1)
                assert hours in {"3h", "12h", "24h", "72h", "168h"}
                cpus, memory, cls, total, backlog, exclusive, gpu = EXPECTED[name]
                args = [arg.strip('"') for arg in queue["worker_args"]]
                assert args[args.index("--group") + 1] == name
                assert args[args.index("--detect-resources") + 1] == "none"
                assert "--min-utilization" not in args
                assert "--idle-timeout" not in args  # Stored separately; generated worker command adds it.
                assert f"--cpus-per-task={cpus}" in queue["additional_args"]
                assert f"--mem={memory}M" in queue["additional_args"]
                assert "--account=def-test" in queue["additional_args"]
                assert ("--exclusive" in queue["additional_args"]) == exclusive
                assert not any(arg.startswith(("--partition", "-p")) for arg in queue["additional_args"])
                assert queue["max_worker_count"] == total and queue["backlog"] == backlog
                assert queue["max_workers_per_alloc"] == 1
                if gpu:
                    assert f"--gres=gpu:{gpu}:1" in queue["additional_args"]
                    assert "--resource" in args and f"{cls}=[0]" in args and "gpus=[0]" in args
                else:
                    assert f"{cls}=sum({cpus})" in args
                assert f"mem=sum({memory})" in args
            assert json.loads(run(binary, root, env, "--output-mode=json", "worker", "list").stdout) == []
    launcher = (ROOT / "configs/hyperqueue_server.sh").read_text()
    assert "#SBATCH --cpus-per-task=1" in launcher and "#SBATCH --mem=4096M" in launcher
    assert "hq journal prune" not in launcher
    print("Nibi: all 65 queues, class pools, resource sizes, shared caps and server request verified")


if __name__ == "__main__":
    main()
