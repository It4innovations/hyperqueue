use crate::common::WrappedRcRefCell;
use crate::server::autoalloc::descriptor::{QueueDescriptor};
use crate::server::autoalloc::AutoAllocResult;
use crate::Map;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::time::{Duration, SystemTime};

const MAX_EVENT_QUEUE_LENGTH: usize = 20;

pub type DescriptorName = String;

pub struct AutoAllocState {
    /// How often should the auto alloc process be invoked?
    refresh_interval: Duration,
    descriptors: Map<DescriptorName, DescriptorState>,
}

impl AutoAllocState {
    pub fn new(refresh_interval: Duration) -> AutoAllocState {
        Self {
            refresh_interval,
            descriptors: Default::default(),
        }
    }

    pub fn refresh_interval(&self) -> Duration {
        self.refresh_interval
    }

    pub fn add_descriptor(
        &mut self,
        name: DescriptorName,
        descriptor: QueueDescriptor,
    ) -> AutoAllocResult<()> {
        if self.descriptors.contains_key(&name) {
            return Err(anyhow::anyhow!(format!(
                "Descriptor {} already exists",
                name
            )));
        }
        self.descriptors
            .insert(name, WrappedRcRefCell::wrap(descriptor).into());
        Ok(())
    }

    pub fn get_descriptor(&self, key: &str) -> Option<&DescriptorState> {
        self.descriptors.get(key)
    }

    pub fn get_descriptor_mut(&mut self, key: &str) -> Option<&mut DescriptorState> {
        self.descriptors.get_mut(key)
    }

    pub fn descriptor_names(&self) -> impl Iterator<Item = &str> {
        self.descriptors.keys().map(|s| s.as_str())
    }
}

/// Represents the state of a single allocation queue.
pub struct DescriptorState {
    pub descriptor: WrappedRcRefCell<QueueDescriptor>,
    /// Allocations that are currently running or are in the queue.
    pub allocations: Vec<Allocation>,
    /// Records events that have occurred on this queue.
    events: VecDeque<AllocationEventHolder>,
}

impl From<WrappedRcRefCell<QueueDescriptor>> for DescriptorState {
    fn from(descriptor: WrappedRcRefCell<QueueDescriptor>) -> Self {
        Self {
            descriptor,
            allocations: Default::default(),
            events: Default::default(),
        }
    }
}

impl DescriptorState {
    pub fn add_event<T: Into<AllocationEventHolder>>(&mut self, event: T) {
        self.events.push_back(event.into());
        if self.events.len() > MAX_EVENT_QUEUE_LENGTH {
            self.events.pop_front();
        }
    }

    pub fn get_events(&self) -> &VecDeque<AllocationEventHolder> {
        &self.events
    }
}

pub type AllocationId = String;

pub struct Allocation {
    pub id: AllocationId,
    pub worker_count: u64,
    pub status: AllocationStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AllocationStatus {
    NotFound,
    Queued { queued_at: SystemTime },
    Running { started_at: SystemTime },
    Finished,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocationEventHolder {
    pub date: SystemTime,
    pub event: AllocationEvent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AllocationEvent {
    AllocationQueued(AllocationId),
    AllocationStarted(AllocationId),
    AllocationFinished(AllocationId),
    AllocationFailed(AllocationId),
    QueueFail { error: String },
    StatusFail { error: String },
}

impl From<AllocationEvent> for AllocationEventHolder {
    fn from(event: AllocationEvent) -> Self {
        Self {
            date: SystemTime::now(),
            event,
        }
    }
}

#[cfg(test)]
mod tests {
    
    
    
    
    
    
    
    

    /*#[test]
    fn test_add_descriptor_with_same_name_twice() {
        let mut state = AutoAllocState::new(Duration::from_secs(1));

        #[async_trait(?Send)]
        impl QueueHandler for () {
            async fn schedule_allocation(
                &self,
                _worker_count: u64,
            ) -> AutoAllocResult<AllocationId> {
                todo!()
            }

            async fn get_allocation_status(
                &self,
                _allocation_id: &str,
            ) -> AutoAllocResult<AllocationStatus> {
                todo!()
            }
        }

        let name = "foo".to_string();
        assert!(state
            .add_descriptor(
                name.clone(),
                Box::new()
            )
            .is_ok());
        assert!(state
            .add_descriptor(
                name,
                WrappedRcRefCell::new_wrapped(Rc::new(RefCell::new(())))
            )
            .is_err());
    }*/
}
