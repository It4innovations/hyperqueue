"""Run benchmark cases sequentially, each in its own process group with a time limit.

Appends one line per case to results.csv: tool,n,rep,submit_s,total_s,status
Writes the currently running case to status.txt.

The plan (main) is the one behind Part 1 of the comparison: N = 10^2..10^5 three times
for every tool (Snakemake only up to 10^4), N = 10^6 once (not Snakemake), and Dask at
10^6 once more with longer connection timeouts (it fails with the defaults).
"""

import os
import signal
import subprocess
import sys
import time

B = os.path.dirname(os.path.abspath(__file__))
LIMIT = 45 * 60
ENV = dict(os.environ, PATH=f"{B}/.venv/bin:" + os.environ["PATH"])


def cmd_for(tool, n):
    if tool == "hq":
        return [f"{B}/run_hq.sh", str(n)]
    if tool == "snakemake":
        return [f"{B}/run_snakemake.sh", str(n)]
    return [f"{B}/.venv/bin/python", f"{B}/py_runtimes.py", tool, str(n)]


def leftovers():
    out = subprocess.run(
        ["pgrep", "-af", r"hq (server|worker) start|bin/snakemake -s|^parsl: |process_worker_pool|raylet|gcs_server"],
        capture_output=True, text=True).stdout.strip()
    return out


def cleanup(pgid):
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(pgid, sig)
        except ProcessLookupError:
            break
        time.sleep(5)
    subprocess.run([f"{B}/.venv/bin/ray", "stop", "--force"], capture_output=True, env=ENV)
    subprocess.run(["pkill", "-f", "^parsl: HTEX interchange"])
    subprocess.run(["pkill", "-f", "process_worker_pool.py"])
    subprocess.run(["bash", "-c", f"rm -rf {B}/hq.* {B}/smk.*"])


def run(tool, n, rep, label=None, env=None):
    left = leftovers()
    if left:
        print(f"WARNING leftover processes before {tool} {n}:\n{left}", flush=True)
    log = f"{B}/logs/{label or tool}-{n}-r{rep}.log"
    with open(f"{B}/status.txt", "w") as f:
        f.write(f"{tool} {n} rep={rep} started={time.time():.0f}\n")
    t0 = time.time()
    with open(log, "w") as out:
        p = subprocess.Popen(cmd_for(tool, n), stdout=out, stderr=subprocess.STDOUT,
                             cwd=B, env=dict(ENV, **(env or {})),
                             start_new_session=True)
        try:
            rc = p.wait(timeout=LIMIT)
            status = "ok" if rc == 0 else f"rc={rc}"
        except subprocess.TimeoutExpired:
            status = "timeout"
    cleanup(p.pid)
    sub = tot = ""
    for line in open(log):
        if line.startswith("RESULT"):
            parts = line.split()
            sub, tot = parts[3], parts[4]
            if tool == "snakemake":
                # dry-run (DAG) time and execution time are reported separately
                tot = f"{float(sub) + float(tot):.3f}"
                done = int(parts[5].split("=")[1])
                if done != n:
                    status = f"incomplete:{done}"
    if not tot and status == "ok":
        status = "noresult"
    line = f"{label or tool},{n},{rep},{sub},{tot},{status}"
    with open(f"{B}/results.csv", "a") as f:
        f.write(line + "\n")
    print(f"RESULT {line} wall={time.time() - t0:.0f}s", flush=True)


def main():
    os.makedirs(f"{B}/logs", exist_ok=True)
    plan = []
    for rep in (1, 2, 3):
        for tool in ("hq", "ray-proc", "dask-proc", "parsl-proc"):
            for n in (100, 1000, 10000, 100000):
                plan.append((tool, n, rep))
        for n in (100, 1000, 10000):
            plan.append(("snakemake", n, rep))
    for tool in ("hq", "ray-proc", "dask-proc", "parsl-proc"):
        plan.append((tool, 1000000, 1))
    for case in plan:
        run(*case)
    run("dask-proc", 1000000, 1, label="dask-proc-timeout600",
        env={"DASK_DISTRIBUTED__COMM__TIMEOUTS__CONNECT": "600s",
             "DASK_DISTRIBUTED__COMM__TIMEOUTS__TCP": "600s"})
    with open(f"{B}/status.txt", "w") as f:
        f.write("ALLDONE\n")
    print("ALLDONE", flush=True)


if __name__ == "__main__":
    main()
