use crate::common::format::human_duration;
use crate::common::utils::time::parse_human_time;
use crate::server::autoalloc::AllocationId;
use crate::server::event::journal::JournalReader;
use crate::server::event::payload::EventPayload;
use crate::transfer::messages::{JobTaskDescription, SubmitRequest};
use anyhow::anyhow;
use chrono::{DateTime, Duration, TimeDelta, Utc};
use clap::{Parser, ValueHint};
use itertools::Itertools;
use std::collections::HashMap;
use std::ops::Add;
use std::path::PathBuf;
use tako::gateway::{ResourceRequest, ResourceRequestVariants};
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

    /// Show only statistics from a given time
    ///
    /// Counted from the start of the server (e.g. "1h")
    #[arg(long)]
    start_time: Option<String>,

    /// Show only statistics upto a given time
    ///
    /// Counted from the start of the server (e.g. "1h")
    #[arg(long)]
    end_time: Option<String>,
}

struct Trace<T> {
    time: Vec<f32>,
    value: Vec<T>,
    start_time: Option<TimeDelta>,
}

impl<T: Copy + Add<T, Output = T>> Trace<T> {
    pub fn new(init_time: TimeDelta, init_value: T, start_time: Option<TimeDelta>) -> Self {
        let init_time = start_time.unwrap_or(TimeDelta::zero()).max(init_time);
        Self {
            time: vec![init_time.as_seconds_f32()],
            value: vec![init_value],
            start_time,
        }
    }
    pub fn add_change(&mut self, time: TimeDelta, delta: T) {
        if let Some(start_time) = self.start_time
            && time < start_time
        {
            let value = &mut self.value[0];
            *value = *value + delta;
        } else {
            let value = *self.value.last().unwrap();
            self.time.push(time.as_seconds_f32());
            self.value.push(value + delta);
        }
    }
    pub fn add_value(&mut self, time: TimeDelta, value: T) {
        if let Some(start_time) = self.start_time
            && time < start_time
        {
            let v = &mut self.value[0];
            *v = value;
        } else {
            self.time.push(time.as_seconds_f32());
            self.value.push(value);
        }
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

    fn to_string(&self, jstats: &JournalStats) -> String {
        use std::fmt::Write;
        let mut out = String::new();
        let mut first = true;
        for (rid, amount) in &self.0 {
            if !first {
                write!(out, ", ").unwrap();
            } else {
                first = false;
            }
            write!(out, "{}:{}", jstats.get_resource_name(*rid), amount).unwrap();
        }
        out
    }
}

enum JobResourceRq {
    Array(ResourceRequestVariants),
    TaskGraph(HashMap<JobTaskId, ResourceRequestVariants>),
}

struct TaskDuration {
    finished: Vec<f32>,
    failed: Vec<f32>,
}

type ResourceId = u32;
struct JournalStats {
    base_time: DateTime<Utc>,
    start_time: Option<Duration>,
    end_time: Option<Duration>,
    resource_names: HashMap<String, ResourceId>,
    worker_resources: HashMap<WorkerId, ResCount>,
    running_workers: HashMap<ResCount, Trace<i32>>,
    w_utilization: HashMap<ResCount, Vec<Trace<f32>>>,
    job_requests: HashMap<JobId, JobResourceRq>,
    queued_workers: Trace<i32>,
    queue_requests: HashMap<AllocationId, i32>,
    running_tasks: HashMap<TaskId, (TimeDelta, smallvec::SmallVec<[WorkerId; 1]>)>,
    durations: HashMap<ResourceRequest, TaskDuration>,
}

impl JournalStats {
    fn new(opts: &JournalReportOpts) -> anyhow::Result<Self> {
        let mut file = JournalReader::open(&opts.journal).map_err(|error| {
            anyhow!(
                "Cannot open event log file at `{}`: {error:?}",
                opts.journal.display()
            )
        })?;

        let start_time = opts
            .start_time
            .as_ref()
            .map(|s| {
                parse_human_time(s)
                    .map(|d| chrono::Duration::from_std(d).unwrap())
                    .map_err(|_| anyhow!("Invalid start time"))
            })
            .transpose()?;
        let end_time = opts
            .end_time
            .as_ref()
            .map(|s| {
                parse_human_time(s)
                    .map(|d| chrono::Duration::from_std(d).unwrap())
                    .map_err(|_| anyhow!("Invalid end time"))
            })
            .transpose()?;

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
            start_time,
            end_time,
            resource_names,
            worker_resources: Default::default(),
            running_workers: Default::default(),
            w_utilization: Default::default(),
            job_requests: Default::default(),
            queued_workers: Trace::new(TimeDelta::zero(), 0, start_time),
            queue_requests: Default::default(),
            running_tasks: Default::default(),
            durations: Default::default(),
        };

        for event in iterator {
            let event = event?;
            let time = event.time - base_time;
            if let Some(end_time) = end_time
                && end_time < time
            {
                break;
            }
            match event.payload {
                EventPayload::WorkerConnected(worker_id, configuration) => {
                    jstats.new_worker(time, worker_id, configuration);
                }
                EventPayload::WorkerLost(worker_id, _) => {
                    let w_resources = jstats.worker_resources.get(&worker_id).unwrap();
                    jstats
                        .running_workers
                        .get_mut(w_resources)
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
                    jstats.task_start_stop(time, task_id, &workers, true, None, false);
                    jstats.running_tasks.insert(task_id, (time, workers));
                }
                EventPayload::TaskFinished { task_id } => {
                    if let Some((task_start_time, workers)) = jstats.running_tasks.remove(&task_id) {
                        jstats.task_start_stop(time, task_id, &workers, false, Some(task_start_time), false);
                    }
                }
                EventPayload::TaskFailed { task_id, .. } => {
                    if let Some((task_start_time, workers)) = jstats.running_tasks.remove(&task_id) {
                        jstats.task_start_stop(time, task_id, &workers, false, Some(task_start_time), true);
                    }
                }
                EventPayload::TasksCanceled { task_ids, .. } => {
                    for task_id in task_ids {
                        if let Some((_, workers)) = jstats.running_tasks.remove(&task_id) {
                            jstats.task_start_stop(time, task_id, &workers, false, None, true);
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
                /*| EventPayload::JobIdle(_)
                | EventPayload::TaskNotify(_)*/
                | EventPayload::ServerStart { .. } => {}
                EventPayload::ServerStop => {
                    for trace in jstats.running_workers.values_mut() {
                        trace.add_value(time, 0);
                    }
                }
            }
        }

        Ok(jstats)
    }

    fn task_start_stop(
        &mut self,
        time: TimeDelta,
        task_id: TaskId,
        workers: &[WorkerId],
        start: bool,
        task_start_time: Option<TimeDelta>,
        fail: bool,
    ) {
        let jrq = self.job_requests.get(&task_id.job_id()).unwrap();
        let rq = match jrq {
            JobResourceRq::Array(rq) => rq,
            JobResourceRq::TaskGraph(map) => map.get(&task_id.job_task_id()).unwrap(),
        };
        if rq.variants.len() != 1 {
            panic!("Resource variants are not yet supported in report");
        }
        let rq = &rq.variants[0];
        if rq.n_nodes > 0 {
            for w in workers {
                let w_resources = self.worker_resources.get(w).unwrap();
                let utilization = self.w_utilization.get_mut(w_resources).unwrap();
                for trace in utilization {
                    trace.add_change(time, if start { 1.0 } else { -1.0 });
                }
            }
        } else {
            assert_eq!(workers.len(), 1);
            let w_resources = self.worker_resources.get(&workers[0]).unwrap();
            let utilization = self.w_utilization.get_mut(w_resources).unwrap();

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
        if let Some(task_start_time) = task_start_time {
            let duration = time - task_start_time;
            if let Some(durations) = self.durations.get_mut(rq) {
                if fail {
                    durations.failed.push(duration.as_seconds_f32());
                } else {
                    durations.finished.push(duration.as_seconds_f32());
                }
            } else {
                let mut finished = Vec::new();
                let mut failed = Vec::new();
                if fail {
                    failed.push(duration.as_seconds_f32());
                } else {
                    finished.push(duration.as_seconds_f32());
                }
                self.durations
                    .insert(rq.clone(), TaskDuration { finished, failed });
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
                .map(|_| Trace::new(time, 0.0, self.start_time))
                .collect_vec();
            self.w_utilization.insert(w_resources.clone(), traces);
        }

        if !self.running_workers.contains_key(&w_resources) {
            self.running_workers
                .insert(w_resources.clone(), Trace::new(time, 0, self.start_time));
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

    pub fn get_resource_name(&self, rid: ResourceId) -> &str {
        self.resource_names
            .iter()
            .find_map(|(name, id)| (*id == rid).then_some(name.as_str()))
            .unwrap_or("<unknown>")
    }
}

#[derive(serde::Serialize)]
struct JsonShape {
    shape: &'static str,
}
#[derive(serde::Serialize)]
struct JsonTimeTrace<'a, T> {
    x: &'a [f32],
    y: &'a [T],
    name: String,
    r#type: &'static str,
    line: JsonShape,
}

#[derive(serde::Serialize)]
struct JsonBoxTrace<'a> {
    x: &'a [f32],
    name: String,
    r#type: &'static str,
}

fn create_report(jstats: &JournalStats) -> String {
    let running_workers_traces = jstats
        .running_workers
        .iter()
        .map(|(wres, trace)| JsonTimeTrace {
            x: &trace.time,
            y: &trace.value,
            name: format!("Running workers {}", wres.to_string(jstats)),
            r#type: "scatter",
            line: JsonShape { shape: "vh" },
        })
        .collect_vec();
    let utilization = jstats
        .w_utilization
        .iter()
        .flat_map(|(wres, traces)| {
            let wres_str = wres.to_string(jstats);
            traces
                .iter()
                .zip(wres.0.iter())
                .map(move |(trace, (rid, _))| JsonTimeTrace {
                    x: &trace.time,
                    y: &trace.value,
                    name: format!(
                        "{name} alloc on {wres}",
                        wres = &wres_str,
                        name = jstats.get_resource_name(*rid)
                    ),
                    r#type: "scatter",
                    line: JsonShape { shape: "vh" },
                })
        })
        .collect_vec();

    let filtering = if jstats.start_time.is_some() || jstats.end_time.is_some() {
        let value = match (jstats.start_time, jstats.end_time) {
            (None, Some(end_time)) => format!("upto {}", human_duration(end_time)),
            (Some(start_time), None) => format!("since {}", human_duration(start_time)),
            (Some(start_time), Some(end_time)) => format!(
                "between {} and {}",
                human_duration(start_time),
                human_duration(end_time)
            ),
            (None, None) => unreachable!(),
        };
        format!(
            r#"<div class="stat-card">
            <div class="stat-label">Time filtering</div>
            <div class="stat-value">{value}</div>
            </div>"#,
        )
    } else {
        "".to_string()
    };

    let durations = jstats
        .durations
        .values()
        .enumerate()
        .flat_map(|(idx, durations)| {
            [
                JsonBoxTrace {
                    x: &durations.finished,
                    name: format!("Finished T{}", idx + 1),
                    r#type: "box",
                },
                JsonBoxTrace {
                    x: &durations.failed,
                    name: format!("Failed T{}", idx + 1),
                    r#type: "box",
                },
            ]
        })
        .filter(|trace| !trace.x.is_empty())
        .collect_vec();

    let duration_terms = jstats
        .durations
        .keys()
        .enumerate()
        .map(|(idx, rq)| {
            let value = resource_rq_to_string(rq);
            format!(
                r#"<span class="term"><span class="term-name">T{idx}</span> = {value}</span>"#,
                idx = idx + 1
            )
        })
        .join("\n");

    let task_names = (1..=jstats.durations.len())
        .map(|idx| format!("T{idx}"))
        .collect_vec();
    let n_finished_tasks = jstats
        .durations
        .values()
        .map(|durations| durations.finished.len())
        .collect_vec();
    let n_failed_tasks = jstats
        .durations
        .values()
        .map(|durations| durations.failed.len())
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

    .chart-terms {
        margin-top: 15px;
        padding-top: 15px;
        border-top: 1px solid #e9ecef;
        font-size: 12px;
        color: #6c757d;
        line-height: 1.4;
    }

    .term {
        display: inline-block;
        margin-right: 20px;
        margin-bottom: 5px;
    }

    .term-name {
        font-weight: 600;
        color: #495057;
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
            <h1 class="report-title">HQ Journal Report</h1>
        </div>

        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">Server started</div>
                <div class="stat-value">{base_time}</div>
            </div>
            {filtering}
        </div>

        <div class="charts-container">
            <div class="chart-wrapper">
                <h3 class="chart-title">Global worker statistics</h3>
                <div id="globalChart"></div>
                <div class="chart-terms">
                    <span class="term"><span class="term-name">Queued workers</span> = Queued & running workers</span>
                    <span class="term"><span class="term-name">Running workers &lt;RESOURCES&gt;</span> = # of running of workers with the given resources</span>
                    <span class="term"><span class="term-name">&lt;RESOURCE&gt alloc on &lt;RESOURCES&gt;</span> = Utilization of a resource on workers with the given resources; normalized: 1.0 = a worker is fully utilized.</span>
                </div>
            </div>
        </div>

        <div class="charts-container">
            <div class="chart-wrapper">
                <h3 class="chart-title">Task counts</h3>
                <div id="countsChart"></div>
                <div class="chart-terms">
                    {duration_terms}
                </div>
            </div>
        </div>

        <div class="charts-container">
            <div class="chart-wrapper">
                <h3 class="chart-title">Task durations</h3>
                <div id="durationsChart"></div>
                <div class="chart-terms">
                    {duration_terms}
                </div>
            </div>
        </div>
    </div>

    <script>
const running_workers = {running_workers};
const utilization = {utilization};
const queued_workers = {{
  x: {queued_workers_x},
  y: {queued_workers_y},
  name: 'Queued workers',
  type: 'scatter',
  line: {{shape: 'vh'}}
}};

const globalData = [queued_workers, ...running_workers, ...utilization];
Plotly.newPlot('globalChart', globalData);

const durationsData = {durations};
Plotly.newPlot('durationsChart', durationsData);

const finished_tasks = {{
  x: {task_names},
  y: {n_finished_tasks},
  name: 'Finished',
  type: 'bar'
}};

const failed_tasks = {{
  x: {task_names},
  y: {n_failed_tasks},
  name: 'Failed',
  type: 'bar'
}};

const countsData = [finished_tasks, failed_tasks];
Plotly.newPlot('countsChart', countsData, {{barmode: 'group'}});
    </script>
</body>
</html>"#,
        base_time = jstats.base_time,
        queued_workers_x = serde_json::to_string(&jstats.queued_workers.time).unwrap(),
        queued_workers_y = serde_json::to_string(&jstats.queued_workers.value).unwrap(),
        running_workers = serde_json::to_string(&running_workers_traces).unwrap(),
        utilization = serde_json::to_string(&utilization).unwrap(),
        durations = serde_json::to_string(&durations).unwrap(),
        task_names = serde_json::to_string(&task_names).unwrap(),
        n_finished_tasks = serde_json::to_string(&n_finished_tasks).unwrap(),
        n_failed_tasks = serde_json::to_string(&n_failed_tasks).unwrap(),
    )
}

fn resource_rq_to_string(rq: &ResourceRequest) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    if rq.n_nodes > 0 {
        write!(out, "nodes: {}", rq.n_nodes).unwrap();
    } else {
        let mut first = true;
        for entry in &rq.resources {
            if !first {
                out.push_str(", ");
            } else {
                first = false;
            }
            write!(out, "{}: {}", entry.resource, entry.policy).unwrap();
        }
    }
    if !rq.min_time.is_zero() {
        write!(
            out,
            ", min_time: {}",
            human_duration(Duration::from_std(rq.min_time).unwrap())
        )
        .unwrap();
    }
    out
}

pub(crate) fn write_journal_report(opts: JournalReportOpts) -> anyhow::Result<()> {
    let stats = JournalStats::new(&opts)?;
    let report = create_report(&stats);
    std::fs::write(opts.output, report)?;
    Ok(())
}
