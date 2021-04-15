use tokio::sync::oneshot;

use crate::common::Set;
use crate::messages::common::WorkerConfiguration;
use crate::messages::worker::WorkerOverview;
use crate::server::task::TaskRef;

pub type WorkerId = u64;

#[derive(Debug)]
pub struct Worker {
    pub id: WorkerId,

    // This is list of assigned tasks
    // !! In case of stealinig T from W1 to W2, T is in "tasks" of W2, even T was not yet canceled from W1.
    pub tasks: Set<TaskRef>,

    pub configuration: WorkerConfiguration,

    pub last_heartbeat: std::time::Instant,

    pub overview_callbacks: Vec<oneshot::Sender<WorkerOverview>>,
}

impl Worker {
    #[inline]
    pub fn id(&self) -> WorkerId {
        self.id
    }

    /*#[inline]
    pub fn address(&self) -> &str {
        &self.listen_address
    }*/

    /*pub fn hostname(&self) -> String {
        let s = self.configuration.listen_address.as_str();
        let s: &str = s.find("://").map(|p| &s[p + 3..]).unwrap_or(s);
        s.chars().take_while(|x| *x != ':').collect()
    }*/

    #[inline]
    pub fn is_underloaded(&self) -> bool {
        let len = self.tasks.len() as u32;
        len < self.configuration.n_cpus
    }
}

//pub type WorkerRef = WrappedRcRefCell<Worker>;

impl Worker {
    pub fn new(id: WorkerId, configuration: WorkerConfiguration) -> Self {
        Worker {
            id,
            configuration,
            tasks: Default::default(),
            overview_callbacks: Default::default(),
            last_heartbeat: std::time::Instant::now(),
        }
    }
}
