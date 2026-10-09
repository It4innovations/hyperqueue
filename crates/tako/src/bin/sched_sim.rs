//! Scheduler simulation driver for the paper's evaluation (see `benchmarks/paper/README.md`).
//!
//! Runs the real scheduling pipeline over synthetic states and writes one CSV row per
//! scheduling round to stdout. All the work lives in `tako::sim`; this is argv parsing only.
//!
//! ```text
//! cargo run --release -p tako --features sim --bin sched_sim -- \
//!     --workers 100 --request-types 8 --tasks 1000,10000,100000 --rounds 5
//! ```
//!
//! Any parameter accepts a comma-separated list; the driver runs the cartesian product.

use tako::server::PruneSchedule;
use tako::sim::{SweepSpec, csv_header, run_sweep};

fn parse_list<T: std::str::FromStr>(value: &str, what: &str) -> Vec<T> {
    value
        .split(',')
        .map(|part| {
            part.trim().parse().unwrap_or_else(|_| {
                eprintln!("Cannot parse {what} value {part:?}");
                std::process::exit(2);
            })
        })
        .collect()
}

fn main() {
    let mut sweep = "sweep".to_string();
    let mut workers = vec![16usize];
    let mut cpus = vec![16u32];
    let mut request_types = vec![1u32];
    let mut priority_levels = vec![1u32];
    let mut tasks = vec![1000usize];
    let mut rounds = 3usize;
    let mut drain_fraction = 0.5f64;
    // Production default; raise it to measure the true solve cost of large instances instead of
    // truncating them at the point where the server would give up.
    let mut mip_time_limit = 5.0f64;
    // usize::MAX disables the global budget, matching `SchedulerConfig`'s default.
    let mut prune_g = vec![usize::MAX];
    let mut prune_f = vec![4usize];
    // The sampling shape for conditions past the fixed prefix. Never changes how many conditions
    // survive, only which, so it is the one pruning knob that holds the model size fixed.
    let mut prune_schedule = vec![PruneSchedule::Quadratic];
    // E2 sets this to 0: prefilled tasks leave the regular queue and would otherwise be
    // indistinguishable from selected ones when counting priority inversions.
    let mut prefill_max = 40u32;
    let mut worker_types: Vec<u32> = Vec::new();
    let mut request_variants = vec![1u32];
    let mut occupied_cpus = 0u32;
    let mut scenario = "uniform".to_string();
    // Cumulative relaxation configurations for the E4 ablation (paper.tex §7).
    let mut relaxations = vec!["all".to_string()];
    // E8: what-if query cost. 0 means "do not query", which is also the control row.
    let mut query_workers = vec![0u32];
    let mut query_types = 1u32;
    let mut query_partial = false;
    // Off by default: it rebuilds the batches unpruned and walks every cut, which must not
    // silently tax the timing sweeps.
    let mut check_pruning = false;
    // Implies --check-pruning, and roughly doubles per-round cost: it solves the unpruned model as
    // well, to establish the throughput the priority rule permits.
    let mut reference_solve = false;
    let mut header = true;
    // Where HiGHS should write its own log, one file per cell and round; the anytime analysis of
    // a truncated solve is read from these.
    let mut mip_log_dir: Option<std::path::PathBuf> = None;

    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < argv.len() {
        let flag = argv[i].as_str();
        if flag == "--no-header" {
            header = false;
            i += 1;
            continue;
        }
        if flag == "--query-partial" {
            query_partial = true;
            i += 1;
            continue;
        }
        if flag == "--check-pruning" {
            check_pruning = true;
            i += 1;
            continue;
        }
        if flag == "--reference-solve" {
            // The reference lives on `PruneError`, which only exists when the check runs, so
            // asking for the reference alone would silently produce nothing.
            reference_solve = true;
            check_pruning = true;
            i += 1;
            continue;
        }
        let Some(value) = argv.get(i + 1) else {
            eprintln!("Missing value for {flag}");
            std::process::exit(2);
        };
        match flag {
            "--sweep" => sweep = value.clone(),
            "--workers" => workers = parse_list(value, "workers"),
            "--cpus" => cpus = parse_list(value, "cpus"),
            "--request-types" => request_types = parse_list(value, "request-types"),
            "--priority-levels" => priority_levels = parse_list(value, "priority-levels"),
            "--tasks" => tasks = parse_list(value, "tasks"),
            "--rounds" => rounds = parse_list(value, "rounds")[0],
            "--drain-fraction" => drain_fraction = parse_list(value, "drain-fraction")[0],
            "--mip-time-limit" => mip_time_limit = parse_list(value, "mip-time-limit")[0],
            "--mip-log-dir" => {
                let dir = std::path::PathBuf::from(value);
                std::fs::create_dir_all(&dir).expect("cannot create --mip-log-dir");
                mip_log_dir = Some(dir);
            }
            "--prune-g" => prune_g = parse_list(value, "prune-g"),
            "--prune-f" => prune_f = parse_list(value, "prune-f"),
            "--prune-schedule" => prune_schedule = parse_list(value, "prune-schedule"),
            "--prefill-max" => prefill_max = parse_list(value, "prefill-max")[0],
            "--worker-types" => worker_types = parse_list(value, "worker-types"),
            "--request-variants" => request_variants = parse_list(value, "request-variants"),
            "--occupied-cpus" => occupied_cpus = parse_list(value, "occupied-cpus")[0],
            "--scenario" => scenario = value.clone(),
            "--query-workers" => query_workers = parse_list(value, "query-workers"),
            "--query-types" => query_types = parse_list(value, "query-types")[0],
            "--relaxations" => {
                relaxations = value.split(',').map(|s| s.trim().to_string()).collect()
            }
            other => {
                eprintln!("Unknown flag {other}");
                std::process::exit(2);
            }
        }
        i += 2;
    }

    if header {
        println!("{}", csv_header());
    }
    for &n_workers in &workers {
        for &cpus_per_worker in &cpus {
            for &n_request_types in &request_types {
                for &levels in &priority_levels {
                    for &n_tasks in &tasks {
                        for &schedule in &prune_schedule {
                            for &g in &prune_g {
                                for &f in &prune_f {
                                    for &variants in &request_variants {
                                        for relaxation in &relaxations {
                                            // strict -> +impossible -> +gaps -> +reservations
                                            let (strict, no_imp, no_gap, no_res) = match relaxation
                                                .as_str()
                                            {
                                                // Relaxation 1 is partly structural, so the strict
                                                // baseline needs `strict_rule` as well as the filter.
                                                "strict" => (true, true, true, true),
                                                // Strict rule enforced, but gaps allowed: the
                                                // only way to see relaxation 2 in isolation,
                                                // since dropping the condition subsumes it.
                                                "strict+gaps" => (true, true, false, true),
                                                "impossible" => (false, false, true, true),
                                                "impossible+gaps" => (false, false, false, true),
                                                "all" => (false, false, false, false),
                                                other => {
                                                    eprintln!("Unknown relaxation set {other:?}");
                                                    std::process::exit(2);
                                                }
                                            };
                                            for &q_workers in &query_workers {
                                                let spec = SweepSpec {
                                                    sweep: sweep.clone(),
                                                    scenario: scenario.clone(),
                                                    n_workers,
                                                    cpus_per_worker,
                                                    n_request_types,
                                                    priority_levels: levels,
                                                    n_tasks,
                                                    rounds,
                                                    drain_fraction,
                                                    mip_time_limit_secs: mip_time_limit,
                                                    mip_log_dir: mip_log_dir.clone(),
                                                    prune_global_max: g,
                                                    prune_fixed_prefix: f,
                                                    prune_schedule: schedule,
                                                    prefill_max,
                                                    worker_types: worker_types.clone(),
                                                    request_variants: variants,
                                                    occupied_cpus_per_worker: occupied_cpus,
                                                    disable_impossible_filter: no_imp,
                                                    disable_gaps: no_gap,
                                                    disable_reservations: no_res,
                                                    strict_rule: strict,
                                                    query_workers: q_workers,
                                                    query_types,
                                                    query_partial,
                                                    check_pruning,
                                                    reference_solve,
                                                };
                                                for row in run_sweep(&spec) {
                                                    println!("{}", row.to_csv());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
