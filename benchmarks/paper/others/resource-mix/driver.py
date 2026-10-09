"""Run the whole benchmark matrix: repetitions x scenarios x arms.

usage: driver.py [REPS [SCENARIO...]]
Each run gets its own process group and a time limit; existing result files are
skipped, so an interrupted run can be resumed. Progress goes to stdout.
"""

import os
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
PY = os.path.join(HERE, ".venv", "bin", "python")
LIMIT = 600

# Both binaries come from ../../e2e/fetch_binaries.py --build-local
E2E_BIN = os.path.join(HERE, "..", "..", "e2e", "bin")
HQ_ARMS = {
    "hq-head": os.path.join(E2E_BIN, "hq-head"),
    "hq-v0.25.1": os.path.join(E2E_BIN, "hq-v0.25.1"),
}
ARMS = ["hq-head", "hq-v0.25.1", "ray", "dask"]
# "B*-split60": flex tasks fixed to the best GPU share found by the LP (workload.best_split)
SCENARIOS = ["A-adv", "A-shuf", "B-var", "B-gpu", "B-split60", "B2-var", "B2-gpu", "B2-split60"]


def command(arm, scenario, output):
    if arm in HQ_ARMS:
        return [PY, "run_hq.py", HQ_ARMS[arm], arm, scenario, output]
    return [PY, f"run_{arm}.py", arm, scenario, output]


def cleanup(pgid):
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(pgid, sig)
        except ProcessLookupError:
            break
        time.sleep(3)
    subprocess.run([os.path.join(HERE, ".venv", "bin", "ray"), "stop", "--force"],
                   capture_output=True)


def main():
    reps = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    scenarios = sys.argv[2:] or SCENARIOS
    os.makedirs(os.path.join(HERE, "logs"), exist_ok=True)
    for rep in range(1, reps + 1):
        for scenario in scenarios:
            for arm in ARMS:
                if scenario.endswith("-var") and arm not in HQ_ARMS:
                    continue  # only HQ supports resource variants
                out = os.path.join(HERE, "results", arm, scenario, f"rep{rep}.json")
                if os.path.exists(out):
                    continue
                os.makedirs(os.path.dirname(out), exist_ok=True)
                log = os.path.join(HERE, "logs", f"{arm}-{scenario}-rep{rep}.log")
                t = time.time()
                with open(log, "w") as f:
                    p = subprocess.Popen(command(arm, scenario, out), cwd=HERE, stdout=f,
                                         stderr=subprocess.STDOUT, start_new_session=True)
                    try:
                        rc = p.wait(timeout=LIMIT)
                        status = "ok" if rc == 0 and os.path.exists(out) else f"rc={rc}"
                    except subprocess.TimeoutExpired:
                        status = "timeout"
                cleanup(p.pid)
                last = open(log).read().strip().splitlines()[-1:] or [""]
                print(f"RUN rep={rep} {scenario} {arm} {status} wall={time.time() - t:.1f}s "
                      f"| {last[0][:120]}", flush=True)
    print("ALLDONE", flush=True)


if __name__ == "__main__":
    main()
