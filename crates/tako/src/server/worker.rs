use std::str;

use crate::scheduler::protocol::WorkerInfo;

pub type WorkerId = u64;

#[derive(Debug)]
pub struct Worker {
    pub id: WorkerId,
    pub ncpus: u32,
    pub listen_address: String,
}

impl Worker {
    #[inline]
    pub fn id(&self) -> WorkerId {
        self.id
    }

    #[inline]
    pub fn address(&self) -> &str {
        &self.listen_address
    }

    pub fn hostname(&self) -> String {
        let s = self.listen_address.as_str();
        let s: &str = s.find("://").map(|p| &s[p + 3..]).unwrap_or(s);
        s.chars().take_while(|x| *x != ':').collect()
    }

    pub fn make_sched_info(&self) -> WorkerInfo {
        WorkerInfo {
            id: self.id,
            n_cpus: self.ncpus,
            hostname: self.hostname(),
        }
    }
}

//pub type WorkerRef = WrappedRcRefCell<Worker>;

impl Worker {
    pub fn new(id: WorkerId, ncpus: u32, listen_address: String) -> Self {
        Worker {
            id,
            ncpus,
            listen_address,
        }
    }
}
