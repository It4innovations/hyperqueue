use crate::define_id_type;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display, Formatter};

define_id_type!(JobId, u32);
define_id_type!(JobTaskId, u32);
define_id_type!(WorkerId, u32);
define_id_type!(InstanceId, u32);

#[derive(Default, Copy, Clone, Hash, PartialOrd, Ord, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskId {
    job_id: JobId,
    job_task_id: JobTaskId,
}

impl Display for TaskId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.job_id, self.job_task_id)
    }
}

impl Debug for TaskId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

impl TaskId {
    #[inline]
    pub fn new(job_id: JobId, job_task_id: JobTaskId) -> Self {
        Self {
            job_id,
            job_task_id,
        }
    }

    #[inline]
    pub fn job_id(&self) -> JobId {
        self.job_id
    }

    #[inline]
    pub fn job_task_id(&self) -> JobTaskId {
        self.job_task_id
    }
}
