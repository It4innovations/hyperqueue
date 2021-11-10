use std::path::PathBuf;
use std::time::Duration;

use derive_builder::Builder;

use crate::common::resources::request::GenericResourceRequests;
use crate::common::resources::{CpuRequest, ResourceRequest};
use crate::common::Map;
use crate::messages::common::{ProgramDefinition, StdioDef, TaskConfiguration};
use crate::messages::gateway::TaskDef;

pub struct GraphBuilder {
    id_counter: u64,
    tasks: Vec<TaskDef>,
}

impl Default for GraphBuilder {
    fn default() -> Self {
        Self {
            id_counter: 1,
            tasks: Default::default(),
        }
    }
}

impl GraphBuilder {
    pub fn task(mut self, builder: TaskConfigBuilder) -> Self {
        let mut config: TaskConfig = builder.build().unwrap();
        config.id = config.id.or_else(|| {
            let id = self.id_counter;
            self.id_counter += 1;
            Some(id)
        });

        let def = build_task_from_config(config);
        self.tasks.push(def);
        self
    }

    pub fn simple_task(self, args: &[&'static str]) -> Self {
        self.task(TaskConfigBuilder::default().args(simple_args(args)))
    }

    pub fn build(self) -> Vec<TaskDef> {
        self.tasks
    }
}

fn build_task_from_config(config: TaskConfig) -> TaskDef {
    let TaskConfig {
        id,
        keep,
        observe,
        time_limit,
        resources,
        args,
        env,
        stdout,
        stderr,
        cwd,
    }: TaskConfig = config;
    let ResourceRequestConfig { cpus, generic }: ResourceRequestConfig = resources.build().unwrap();

    let program_def = ProgramDefinition {
        args: args.into_iter().map(|v| v.into()).collect(),
        env: env.into_iter().map(|(k, v)| (k.into(), v.into())).collect(),
        stdout,
        stderr,
        cwd,
    };
    let body = rmp_serde::to_vec(&program_def).unwrap();

    let conf = TaskConfiguration {
        resources: ResourceRequest::new(cpus, Default::default(), generic),
        n_outputs: 0,
        time_limit,
        body,
    };
    TaskDef {
        id: id.unwrap_or(1).into(),
        conf,
        priority: 0,
        keep,
        observe: observe.unwrap_or(true),
    }
}

pub fn build_task(config: TaskConfigBuilder) -> TaskDef {
    build_task_from_config(config.build().unwrap())
}

#[derive(Builder, Default, Clone)]
#[builder(pattern = "owned")]
pub struct TaskConfig {
    #[builder(default)]
    id: Option<u64>,

    #[builder(default)]
    keep: bool,
    #[builder(default)]
    observe: Option<bool>,

    #[builder(default)]
    time_limit: Option<Duration>,

    #[builder(default)]
    resources: ResourceRequestConfigBuilder,

    #[builder(default)]
    args: Vec<String>,
    #[builder(default)]
    env: Map<String, String>,
    #[builder(default)]
    stdout: StdioDef,
    #[builder(default)]
    stderr: StdioDef,
    #[builder(default)]
    cwd: Option<PathBuf>,
}

#[derive(Builder, Default, Clone)]
#[builder(pattern = "owned", derive(Clone))]
pub struct ResourceRequestConfig {
    #[builder(default)]
    cpus: CpuRequest,
    #[builder(default)]
    generic: GenericResourceRequests,
}

pub fn simple_args(args: &[&'static str]) -> Vec<String> {
    args.iter().map(|&v| v.to_string()).collect()
}

pub fn simple_task(args: &[&'static str], id: u64) -> TaskDef {
    let config = TaskConfigBuilder::default()
        .args(simple_args(args))
        .id(Some(id));
    build_task(config)
}
