use std::path::PathBuf;
use std::time::Duration;

use crate::common::index::ItemId;
use derive_builder::Builder;

use crate::common::resources::CpuRequest;
use crate::common::Map;
use crate::messages::common::{ProgramDefinition, StdioDef};
use crate::messages::gateway::{
    GenericResourceRequest, ResourceRequest, SharedTaskConfiguration, TaskConfiguration,
};
use crate::TaskId;

pub struct GraphBuilder {
    id_counter: u64,
    tasks: Vec<TaskConfiguration>,
    configurations: Vec<SharedTaskConfiguration>,
}

impl Default for GraphBuilder {
    fn default() -> Self {
        Self {
            id_counter: 1,
            tasks: Default::default(),
            configurations: Default::default(),
        }
    }
}

impl GraphBuilder {
    pub fn singleton(
        builder: TaskConfigBuilder,
    ) -> (Vec<TaskConfiguration>, Vec<SharedTaskConfiguration>) {
        GraphBuilder::default().task(builder).build()
    }

    pub fn tasks(self, builders: impl Iterator<Item = TaskConfigBuilder>) -> Self {
        let mut s = self;
        for builder in builders {
            s = s.task(builder);
        }
        s
    }

    pub fn task(mut self, builder: TaskConfigBuilder) -> Self {
        let mut config: TaskConfig = builder.build().unwrap();
        config.id = config.id.or_else(|| {
            let id = self.id_counter;
            self.id_counter += 1;
            Some(id)
        });

        let (mut tdef, tconf) = build_task_def_from_config(config);
        tdef.shared_data_index = self.configurations.len() as u32;
        self.tasks.push(tdef);
        self.configurations.push(tconf);
        self
    }

    pub fn simple_task(self, args: &[&'static str]) -> Self {
        self.task(TaskConfigBuilder::default().args(simple_args(args)))
    }

    pub fn build(self) -> (Vec<TaskConfiguration>, Vec<SharedTaskConfiguration>) {
        (self.tasks, self.configurations)
    }
}

pub fn build_task_def_from_config(
    config: TaskConfig,
) -> (TaskConfiguration, SharedTaskConfiguration) {
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
        stdin: vec![],
        cwd,
    };
    let body = rmp_serde::to_vec(&program_def).unwrap();

    let conf = SharedTaskConfiguration {
        resources: ResourceRequest {
            cpus,
            min_time: Default::default(),
            generic,
        },
        n_outputs: 0,
        time_limit,
        priority: 0,
        keep,
        observe: observe.unwrap_or(true),
    };
    (
        TaskConfiguration {
            id: TaskId::new(id.unwrap_or(1) as <TaskId as ItemId>::IdType),
            shared_data_index: 0,
            task_deps: Vec::new(),
            body,
        },
        conf,
    )
}

#[derive(Builder, Default, Clone)]
#[builder(pattern = "owned")]
pub struct TaskConfig {
    #[builder(default)]
    pub id: Option<u64>,

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
    #[builder(default = "std::env::current_dir().unwrap()")]
    cwd: PathBuf,
}

#[derive(Builder, Default, Clone)]
#[builder(pattern = "owned", derive(Clone))]
pub struct ResourceRequestConfig {
    #[builder(default)]
    cpus: CpuRequest,
    #[builder(default)]
    generic: Vec<GenericResourceRequest>,
}

pub fn simple_args(args: &[&'static str]) -> Vec<String> {
    args.iter().map(|&v| v.to_string()).collect()
}

pub fn simple_task(args: &[&'static str], id: u64) -> TaskConfigBuilder {
    TaskConfigBuilder::default()
        .args(simple_args(args))
        .id(Some(id))
}
