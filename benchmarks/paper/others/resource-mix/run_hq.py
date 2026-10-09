"""Run one scenario on HyperQueue.

usage: run_hq.py HQ_BINARY ARM SCENARIO OUTPUT_JSON [DIV]
"""

import json
import os
import subprocess
import sys
import tempfile
import time

import workload

HERE = os.path.dirname(os.path.abspath(__file__))


def job_file(sc, path):
    lines = []
    for t in sc.tasks:
        durs = [f'"{d:.4f}"' for _, _, d in t.variants]
        lines.append("[[task]]")
        lines.append(f"id = {t.tid}")
        lines.append(f'command = ["{HERE}/task.sh", "{t.tid}", {", ".join(durs)}]')
        lines.append('stdout = "/dev/null"')
        lines.append('stderr = "/dev/null"')
        for c, g, _ in t.variants:
            res = f'"cpus" = {c}' + (f', "gpus" = {g}' if g else "")
            lines.append("[[task.request]]")
            lines.append(f"resources = {{ {res} }}")
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")


def main():
    binary, arm, name, output = sys.argv[1:5]
    div = float(sys.argv[5]) if len(sys.argv) > 5 else workload.DIV
    sc = workload.scenario(name, div)
    work = tempfile.mkdtemp(prefix="hq-", dir=os.path.dirname(os.path.abspath(output)))
    records = os.path.join(work, "records.txt")
    env = dict(os.environ, HQ_SERVER_DIR=os.path.join(work, "server"), RECORDS=records)
    job_file(sc, os.path.join(work, "job.toml"))

    procs = []
    log = open(os.path.join(work, "hq.log"), "w")

    def spawn(args, extra_env=None):
        p = subprocess.Popen([binary] + args, env=dict(env, **(extra_env or {})),
                             stdout=log, stderr=subprocess.STDOUT, cwd=work)
        procs.append(p)

    def hq(args):
        return subprocess.run([binary] + args, env=env, cwd=work, check=True,
                              capture_output=True, text=True).stdout

    def start_worker(label, cpus, gpus):
        args = ["worker", "start", f"--cpus={cpus}", "--detect-resources=none"]
        if gpus:
            args.append("--resource=gpus=[" + ",".join(map(str, range(gpus))) + "]")
        else:
            # Workaround for a v0.25.1 panic (workerload.rs:175, index out of bounds):
            # when no variant fits right now, v0.25.1 picks a variant round-robin and may
            # account the GPU variant on a CPU-only worker, whose resource vector is too
            # short to hold "gpus". An unused resource registered after "gpus" (hence the
            # GPU workers connect first) makes the vector long enough; "gpus" is then 0.
            args.append("--resource=pad=sum(1)")
        spawn(args, {"WORKER_LABEL": label})

    spawn(["server", "start"])
    while not os.path.exists(os.path.join(work, "server", "hq-current", "access.json")):
        time.sleep(0.05)

    def wait_for_workers(count):
        while len(json.loads(hq(["worker", "list", "--output-mode=json"]))) < count:
            time.sleep(0.1)

    # Workers with GPUs connect first (see the workaround in start_worker)
    late = []
    started = 0
    for with_gpus in (True, False):
        for label, ntype, cpus, gpus in workload.nodes():
            if bool(gpus) != with_gpus:
                continue
            if ntype in sc.late_nodes:
                late.append((sc.late_nodes[ntype], label, cpus, gpus))
            else:
                start_worker(label, cpus, gpus)
                started += 1
        wait_for_workers(started)

    t0 = time.time()
    hq(["job", "submit-file", "job.toml"])
    t_submit = time.time()
    if late:
        late.sort()
        waiter = subprocess.Popen([binary, "job", "wait", "all"], env=env, cwd=work,
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for at, label, cpus, gpus in late:
            time.sleep(max(0.0, t0 + at - time.time()))
            start_worker(label, cpus, gpus)
        waiter.wait()
    else:
        hq(["job", "wait", "all"])
    t_end = time.time()

    hq(["server", "stop"])
    for p in procs:
        try:
            p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            p.kill()

    recs = []
    for line in open(records):
        tid, v, label, s, e = line.split()
        recs.append([int(tid), int(v), label, float(s.replace(",", ".")), float(e.replace(",", "."))])
    out = {"arm": arm, "scenario": name, "div": div, "t0": t0, "t_submit": t_submit,
           "t_end": t_end, "records": recs}
    with open(output, "w") as f:
        json.dump(out, f)
    subprocess.run(["rm", "-rf", work])
    print(f"{arm} {name}: tasks={len(recs)}/{len(sc.tasks)} "
          f"makespan={max(r[4] for r in recs) - t0:.2f}s submit={t_submit - t0:.3f}s")


if __name__ == "__main__":
    main()
