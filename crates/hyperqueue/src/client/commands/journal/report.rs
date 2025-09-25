use crate::server::autoalloc::AllocationId;
use crate::server::event::journal::JournalReader;
use crate::server::event::payload::EventPayload;
use crate::server::event::Event;
use crate::transfer::messages::{JobTaskDescription, SubmitRequest};
use anyhow::anyhow;
use chrono::{DateTime, TimeDelta, Utc};
use clap::{Parser, ValueHint};
use itertools::Itertools;
use nix::libc::time;
use std::alloc::alloc;
use std::collections::HashMap;
use std::fmt::Display;
use std::ops::Add;
use std::path::PathBuf;
use tako::gateway::ResourceRequestVariants;
use tako::resources::ResourceAmount;
use tako::worker::WorkerConfiguration;
use tako::{JobId, JobTaskId, TaskId, WorkerId};

#[derive(Parser)]
pub(crate) struct JournalReportOpts {
    /// Path to a journal
    #[arg(value_hint = ValueHint::FilePath)]
    journal: PathBuf,

    /// Path to an output file
    #[arg(value_hint = ValueHint::FilePath)]
    output: PathBuf,
}

struct Trace<T> {
    time: Vec<f32>,
    value: Vec<T>,
}

impl<T: Copy + Add<T, Output = T>> Trace<T> {
    pub fn new(init_time: TimeDelta, init_value: T) -> Self {
        Self {
            time: vec![init_time.as_seconds_f32()],
            value: vec![init_value],
        }
    }
    pub fn add_change(&mut self, time: TimeDelta, delta: T) {
        let value = *self.value.last().unwrap();
        self.time.push(time.as_seconds_f32());
        self.value.push(value + delta);
    }
    pub fn add_value(&mut self, time: TimeDelta, value: T) {
        self.time.push(time.as_seconds_f32());
        self.value.push(value);
    }
}

#[derive(PartialEq, Eq, Hash, Clone)]
struct ResCount(Vec<(ResourceId, ResourceAmount)>);

impl ResCount {
    fn new(mut wres: Vec<(ResourceId, ResourceAmount)>) -> Self {
        wres.sort_unstable();
        ResCount(wres)
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

impl Display for ResCount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut first = true;
        for (rid, amount) in &self.0 {
            if !first {
                write!(f, ", ")?;
            } else {
                first = false;
            }
            write!(f, "{}: {}", rid, amount)?;
        }
        Ok(())
    }
}

enum JobResourceRq {
    Array(ResourceRequestVariants),
    TaskGraph(HashMap<JobTaskId, ResourceRequestVariants>),
}

type ResourceId = u32;
struct JournalStats {
    base_time: DateTime<Utc>,
    resource_names: HashMap<String, ResourceId>,
    worker_resources: HashMap<WorkerId, ResCount>,
    running_workers: HashMap<ResCount, Trace<i32>>,
    w_utilization: HashMap<ResCount, Vec<Trace<f32>>>,
    job_requests: HashMap<JobId, JobResourceRq>,
    queued_workers: Trace<i32>,
    queue_requests: HashMap<AllocationId, i32>,
    running_tasks: HashMap<TaskId, smallvec::SmallVec<[WorkerId; 1]>>,
}

impl JournalStats {
    fn new(opts: &JournalReportOpts) -> anyhow::Result<Self> {
        let mut file = JournalReader::open(&opts.journal).map_err(|error| {
            anyhow!(
                "Cannot open event log file at `{}`: {error:?}",
                opts.journal.display()
            )
        })?;
        let mut iterator = &mut file;
        let first_event = iterator.next().ok_or(anyhow!("Empty journal"))??;
        if !matches!(first_event.payload, EventPayload::ServerStart { .. }) {
            return Err(anyhow!("Journal does not start with server start event"));
        }
        let base_time = first_event.time;

        let mut resource_names = HashMap::new();
        resource_names.insert("cpus".to_string(), 0);

        let mut jstats = JournalStats {
            base_time,
            resource_names,
            worker_resources: Default::default(),
            running_workers: Default::default(),
            w_utilization: Default::default(),
            job_requests: Default::default(),
            queued_workers: Trace::new(TimeDelta::zero(), 0),
            queue_requests: Default::default(),
            running_tasks: Default::default(),
        };

        for event in iterator {
            let event = event?;
            let time = base_time - event.time;
            match event.payload {
                EventPayload::WorkerConnected(worker_id, configuration) => {
                    jstats.new_worker(time, worker_id, configuration);
                }
                EventPayload::WorkerLost(worker_id, _) => {
                    let w_resources = jstats.worker_resources.remove(&worker_id).unwrap();
                    jstats
                        .running_workers
                        .get_mut(&w_resources)
                        .unwrap()
                        .add_change(time, -1);
                }
                EventPayload::WorkerOverviewReceived(_) => {}
                EventPayload::Submit {
                    job_id,
                    serialized_desc,
                    closed_job: _,
                } => {
                    let submit_request: SubmitRequest = serialized_desc.deserialize()?;
                    jstats.new_submit(job_id, submit_request)
                }
                EventPayload::JobCompleted(_) => {}
                EventPayload::JobOpen(_, _) => {}
                EventPayload::JobClose(_) => {}
                EventPayload::TaskStarted {
                    task_id, workers, ..
                } => {
                    jstats.task_start_stop(time, task_id, &workers, true);
                    jstats.running_tasks.insert(task_id, workers);
                }
                EventPayload::TaskFinished { task_id }
                | EventPayload::TaskFailed { task_id, .. } => {
                    if let Some(workers) = jstats.running_tasks.remove(&task_id) {
                        jstats.task_start_stop(time, task_id, &workers, false);
                    }
                }
                EventPayload::TasksCanceled { task_ids, .. } => {
                    for task_id in task_ids {
                        if let Some(workers) = jstats.running_tasks.remove(&task_id) {
                            jstats.task_start_stop(time, task_id, &workers, false);
                        }
                    }
                }
                EventPayload::AllocationQueueCreated(_, _)
                | EventPayload::AllocationQueueRemoved(_) => {}
                EventPayload::AllocationFinished(queue_id, allocation_id) => {
                    let worker_count =
                        jstats
                            .queue_requests
                            .remove(&allocation_id)
                            .ok_or_else(|| {
                                anyhow!("Allocation `{}` does not exist in the journal", queue_id)
                            })?;
                    jstats.queued_workers.add_change(time, -worker_count);
                }
                EventPayload::AllocationQueued {
                    allocation_id,
                    worker_count,
                    ..
                } => {
                    jstats
                        .queue_requests
                        .insert(allocation_id, worker_count as i32);
                    jstats.queued_workers.add_change(time, worker_count as i32);
                }
                EventPayload::AllocationStarted(_, _)
                | EventPayload::JobIdle(_)
                | EventPayload::TaskNotify(_)
                | EventPayload::ServerStart { .. } => {}
                EventPayload::ServerStop => {
                    for trace in jstats.running_workers.values_mut() {
                        trace.add_value(time, 0);
                    }
                }
            }
        }

        Ok(todo!())
    }

    fn task_start_stop(
        &mut self,
        time: TimeDelta,
        task_id: TaskId,
        workers: &[WorkerId],
        start: bool,
    ) {
        let jrq = self.job_requests.get(&task_id.job_id()).unwrap();
        let rq = match jrq {
            JobResourceRq::Array(rq) => &rq,
            JobResourceRq::TaskGraph(map) => map.get(&task_id.job_task_id()).unwrap(),
        };
        if rq.variants.len() != 1 {
            panic!("Resource variants are not yet supported in report");
        }
        let rq = &rq.variants[0];
        if rq.n_nodes > 0 {
            for w in workers {
                let w_resources = self.worker_resources.get(&w).unwrap();
                let utilization = self.w_utilization.get_mut(&w_resources).unwrap();
                for trace in utilization {
                    trace.add_change(time, if start { 1.0 } else { -1.0 });
                }
            }
        } else {
            assert_eq!(workers.len(), 1);
            let w_resources = self.worker_resources.get(&workers[0]).unwrap();
            let utilization = self.w_utilization.get_mut(&w_resources).unwrap();

            for t in rq.resources.iter() {
                let res_id = *self.resource_names.get_mut(&t.resource).unwrap();
                let pos = w_resources
                    .0
                    .iter()
                    .position(|(rid, _)| *rid == res_id)
                    .unwrap();
                let u = t
                    .policy
                    .amount_or_none_if_all()
                    .map(|a| a.as_f32() / w_resources.0[pos].1.as_f32())
                    .unwrap_or(1.0);
                utilization[pos].add_change(time, if start { u } else { -u });
            }
        }
    }

    fn new_submit(&mut self, job_id: JobId, submit: SubmitRequest) {
        let rq = match submit.submit_desc.task_desc {
            JobTaskDescription::Array { task_desc, .. } => {
                JobResourceRq::Array(task_desc.resources)
            }
            JobTaskDescription::Graph { tasks } => {
                let map = tasks
                    .into_iter()
                    .map(|t| (t.id, t.task_desc.resources))
                    .collect();
                JobResourceRq::TaskGraph(map)
            }
        };
        self.job_requests.insert(job_id, rq);
    }

    fn new_worker(
        &mut self,
        time: TimeDelta,
        worker_id: WorkerId,
        configuration: Box<WorkerConfiguration>,
    ) {
        let mut w_resources = Vec::new();
        for resource in &configuration.resources.resources {
            w_resources.push((self.get_resource_id(&resource.name), resource.kind.size()));
        }
        let w_resources = ResCount::new(w_resources);

        if !self.w_utilization.contains_key(&w_resources) {
            let traces = (0..w_resources.len())
                .map(|_| Trace::new(time, 0.0))
                .collect_vec();
            self.w_utilization.insert(w_resources.clone(), traces);
        }

        if !self.running_workers.contains_key(&w_resources) {
            self.running_workers
                .insert(w_resources.clone(), Trace::new(time, 0));
        };
        self.running_workers
            .get_mut(&w_resources)
            .unwrap()
            .add_change(time, 1);
        self.worker_resources.insert(worker_id, w_resources);
    }

    pub fn get_resource_id(&mut self, name: &str) -> ResourceId {
        self.resource_names
            .get_mut(name)
            .copied()
            .unwrap_or_else(|| {
                let resource_id = self.resource_names.len() as ResourceId;
                self.resource_names.insert(name.to_string(), resource_id);
                resource_id
            })
    }
}
/*
fn gather_journal_stats(opts: &JournalReportOpts) -> anyhow::Result<JournalStats> {
    let mut file = JournalReader::open(&opts.journal).map_err(|error| {
        anyhow!(
            "Cannot open event log file at `{}`: {error:?}",
            opts.journal.display()
        )
    })?;
    let mut resource_names: HashMap<String, u32> = HashMap::new();

    let get_resource_id = |resource_names, name: &str| {
        resource_names
            .entry(name)
            .or_insert_with(|| resource_names.len() as u32)
    };

    let mut running_workers: HashMap<ResCount, Trace<i32>> = HashMap::new();
    let mut queued_workers: Trace<i32> = Trace::new(TimeDelta::zero(), 0);
    let mut utilizations: HashMap<(ResCount, String), Trace<f32>> = HashMap::new();

    let mut worker_resources: HashMap<WorkerId, ResCount> = HashMap::new();
    let mut job_requests: HashMap<JobId, JobResourceRq> = HashMap::new();
    let mut q_workers: HashMap<AllocationId, i32> = HashMap::new();

    let mut iterator = &mut file;
    let first_event = iterator.next().ok_or(anyhow!("Empty journal"))??;
    if !matches!(first_event.payload, EventPayload::ServerStart { .. }) {
        return Err(anyhow!("Journal does not start with server start event"));
    }
    let base_time = first_event.time;

    todo!()
    /*    Ok(JournalStats {
        running_workers,
        queued_workers,
    })*/
}*/
/*
 x: {running_workers_x},
 y: {running_workers_y},
 name: 'Running workers',
 type: 'scatter',
 line: {{shape: 'vh'}}
*/

#[derive(serde::Serialize)]
struct JsonShape {
    shape: &'static str,
}
#[derive(serde::Serialize)]
struct JsonTrace<'a, T> {
    x: &'a [f32],
    y: &'a [T],
    name: String,
    r#type: &'static str,
    line: JsonShape,
}

fn create_report(jstats: &JournalStats) -> String {
    let running_workers_traces = jstats
        .running_workers
        .iter()
        .map(|(wres, trace)| JsonTrace {
            x: &trace.time,
            y: &trace.value,
            name: format!("Running workers ({})", wres),
            r#type: "scatter",
            line: JsonShape { shape: "vh" },
        })
        .collect_vec();
    let head = r#"
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>HQ Journal Report</title>
<script src='https://cdn.plot.ly/plotly-3.1.0.min.js'></script>
<style>
    * {
        margin: 0;
        padding: 0;
        box-sizing: border-box;
    }

    body {
        font-family: sans-serif;
        line-height: 1.6;
        color: #333;
        background-color: #f8f9fa;
        padding: 20px;
    }

    .container {
        max-width: 1200px;
        margin: 0 auto;
        background: white;
        padding: 30px;
        border-radius: 8px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
    }

    .header {
        text-align: center;
        margin-bottom: 30px;
        padding-bottom: 20px;
        border-bottom: 2px solid #e9ecef;
    }

    .logo {
        width: 80px;
        height: 80px;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 50%;
        margin: 0 auto 15px;
        display: flex;
        align-items: center;
        justify-content: center;
        color: white;
        font-size: 24px;
        font-weight: bold;
    }

    .report-title {
        font-size: 28px;
        color: #2c3e50;
        margin-bottom: 10px;
    }

    .stats-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
        gap: 20px;
        margin-bottom: 40px;
    }

    .stat-card {
        background: #f8f9fa;
        padding: 20px;
        border-radius: 6px;
        border-left: 4px solid #667eea;
    }

    .stat-label {
        font-size: 14px;
        color: #6c757d;
        margin-bottom: 5px;
        font-weight: 500;
    }

    .stat-value {
        font-size: 18px;
        font-weight: 600;
        color: #2c3e50;
    }

    .charts-container {
        display: flex;
        flex-direction: column;
        gap: 30px;
        margin-top: 30px;
    }

    .chart-wrapper {
        background: white;
        padding: 20px;
        border-radius: 6px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        width: 100%;
    }

    .chart-title {
        font-size: 18px;
        font-weight: 600;
        color: #2c3e50;
        margin-bottom: 15px;
        text-align: center;
    }

    canvas {
        max-width: 100%;
        height: auto;
    }
</style>
</head>"#;

    format!(
        r#"<!DOCTYPE html>
<html lang="en">
{head}
<body>
    <div class="container">
        <div class="header">
            <div class="logo">SM</div>
            <h1 class="report-title">HQ Journal Report</h1>
        </div>

        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">Report Generated</div>
                <div class="stat-value" id="reportDate"></div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Monitoring Period</div>
                <div class="stat-value">Sept 18 - Sept 25, 2025</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Server UUID</div>
                <div class="stat-value">a7f2-8b4e-9c1d-3a5f</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Data Points</div>
                <div class="stat-value">168 hours</div>
            </div>
        </div>

        <div class="charts-container">
            <div class="chart-wrapper">
                <h3 class="chart-title">Global statistics</h3>
                <div id="globalChart"></div>
            </div>
        </div>
    </div>

    <script>
const running_workers = {running_workers};
const queued_workers = {{
  x: {queued_workers_x},
  y: {queued_workers_y},
  name: 'Queued workers',
  type: 'scatter',
  line: {{shape: 'vh'}}
}};

const data = [queued_workers, ..running_workers];
Plotly.newPlot('globalChart', data);
    </script>
</body>
</html>"#,
        queued_workers_x = serde_json::to_string(&jstats.queued_workers.time).unwrap(),
        queued_workers_y = serde_json::to_string(&jstats.queued_workers.value).unwrap(),
        running_workers = serde_json::to_string(&running_workers_traces).unwrap(),
    )
}

pub(crate) fn write_journal_report(opts: JournalReportOpts) -> anyhow::Result<()> {
    let stats = JournalStats::new(&opts)?;
    let report = create_report(&stats);
    std::fs::write(opts.output, report)?;
    Ok(())
}
