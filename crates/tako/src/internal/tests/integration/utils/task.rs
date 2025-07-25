use bincode::Options;
use std::path::PathBuf;
use std::time::Duration;

use derive_builder::Builder;
use smallvec::smallvec;
use thin_vec::ThinVec;

use crate::TaskId;
use crate::gateway::{
    CrashLimit, ResourceRequest, ResourceRequestEntry, ResourceRequestVariants,
    SharedTaskConfiguration, TaskConfiguration, TaskDataFlags,
};
use crate::internal::common::Map;
use crate::internal::common::resources::NumOfNodes;
use crate::program::{ProgramDefinition, StdioDef};
use crate::resources::{AllocationRequest, ResourceAmount};

pub struct GraphBuilder {
    id_counter: u32,
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

    pub fn task(mut self, builder: TaskConfigBuilder) -> Self {
        let mut config: TaskConfig = builder.build().unwrap();
        config.id = config.id.or_else(|| {
            let id = self.id_counter;
            self.id_counter += 1;
            Some(id)
        });
        self.add_task_from_config(config);
        self
    }

    pub fn task_copied(mut self, builder: TaskConfigBuilder, count: u64) -> Self {
        let config = builder.build().unwrap();
        for _ in 0..count {
            let mut config: TaskConfig = config.clone();
            assert!(config.id.is_none());
            config.id = {
                let id = self.id_counter;
                self.id_counter += 1;
                Some(id)
            };
            self.add_task_from_config(config);
        }
        self
    }

    pub fn simple_task(self, args: &[&'static str]) -> Self {
        self.task(TaskConfigBuilder::default().args(simple_args(args)))
    }

    fn add_task_from_config(&mut self, config: TaskConfig) {
        let (mut tdef, tconf) = build_task_def_from_config(config);
        tdef.shared_data_index = self.configurations.len() as u32;
        self.tasks.push(tdef);
        self.configurations.push(tconf);
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
        time_limit,
        resources,
        args,
        env,
        stdout,
        stderr,
        cwd,
    }: TaskConfig = config;
    let ResourceRequestConfig {
        n_nodes,
        entries,
        min_time,
    }: ResourceRequestConfig = resources.build().unwrap();

    let program_def = ProgramDefinition {
        args: args.into_iter().map(|v| v.into()).collect(),
        env: env.into_iter().map(|(k, v)| (k.into(), v.into())).collect(),
        stdout,
        stderr,
        stdin: vec![],
        cwd,
    };
    let body = bincode::DefaultOptions::new()
        .with_limit(crate::MAX_FRAME_SIZE as u64)
        .serialize(&program_def)
        .unwrap();

    let conf = SharedTaskConfiguration {
        resources: ResourceRequestVariants {
            variants: smallvec![ResourceRequest {
                n_nodes,
                resources: entries.into(),
                min_time,
            }],
        },
        time_limit,
        priority: 0,
        crash_limit: CrashLimit::default(),
        data_flags: TaskDataFlags::empty(),
        body: body.into_boxed_slice(),
    };
    (
        TaskConfiguration {
            id: TaskId::new_test(id.unwrap_or(1)),
            shared_data_index: 0,
            task_deps: ThinVec::new(),
            dataobj_deps: ThinVec::new(),
            entry: None,
        },
        conf,
    )
}

#[derive(Builder, Default, Clone)]
#[builder(pattern = "owned")]
pub struct TaskConfig {
    #[builder(default)]
    id: Option<u32>,

    #[builder(default)]
    time_limit: Option<Duration>,

    #[builder(default = "ResourceRequestConfigBuilder::default().cpus(1)")]
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
    n_nodes: NumOfNodes,
    #[builder(default)]
    entries: Vec<ResourceRequestEntry>,
    #[builder(default)]
    min_time: Duration,
}

impl ResourceRequestConfigBuilder {
    pub fn cpus<A: Into<ResourceAmount>>(self, n_cpus: A) -> Self {
        self.add_compact("cpus", n_cpus)
    }

    pub fn _add(&mut self, name: &str, request: AllocationRequest) {
        if self.entries.is_none() {
            self.entries = Some(vec![]);
        }

        self.entries.as_mut().unwrap().push(ResourceRequestEntry {
            resource: name.to_string(),
            policy: request,
        })
    }

    pub fn add_compact<A: Into<ResourceAmount>>(mut self, name: &str, amount: A) -> Self {
        self._add(name, AllocationRequest::Compact(amount.into()));
        self
    }

    pub fn add_force_compact<A: Into<ResourceAmount>>(mut self, name: &str, amount: A) -> Self {
        self._add(name, AllocationRequest::ForceCompact(amount.into()));
        self
    }
}

pub fn simple_args(args: &[&'static str]) -> Vec<String> {
    args.iter().map(|&v| v.to_string()).collect()
}

pub fn simple_task(args: &[&'static str], id: u32) -> TaskConfigBuilder {
    TaskConfigBuilder::default()
        .args(simple_args(args))
        .id(Some(id))
}
