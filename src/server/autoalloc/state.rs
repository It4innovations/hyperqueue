use crate::server::autoalloc::descriptor::QueueDescriptor;
use crate::server::autoalloc::{AutoAllocError, AutoAllocResult};
use crate::Map;
use std::time::{Duration, Instant};

pub struct AutoAllocState {
    refresh_interval: Duration,
    descriptors: Map<String, DescriptorState>,
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
        name: String,
        descriptor: Box<dyn QueueDescriptor>,
    ) -> AutoAllocResult<()> {
        if self.descriptors.contains_key(&name) {
            return Result::Err(AutoAllocError::DescriptorAlreadyExists(name));
        }
        self.descriptors.insert(name, descriptor.into());
        Ok(())
    }

    pub fn descriptors(&mut self) -> impl Iterator<Item = &mut DescriptorState> {
        self.descriptors.values_mut()
    }
}

pub struct DescriptorState {
    pub descriptor: Box<dyn QueueDescriptor>,
    pub allocations: Vec<Allocation>,
}

impl From<Box<dyn QueueDescriptor>> for DescriptorState {
    fn from(descriptor: Box<dyn QueueDescriptor>) -> Self {
        Self {
            descriptor,
            allocations: Default::default(),
        }
    }
}

pub type AllocationId = String;

pub struct Allocation {
    pub id: AllocationId,
    pub worker_count: u64,
    pub status: AllocationStatus,
}

pub enum AllocationStatus {
    Queued { queued_at: Instant },
    Running { started_at: Instant },
}

#[cfg(test)]
mod tests {
    use crate::server::autoalloc::descriptor::QueueDescriptor;
    use crate::server::autoalloc::state::{AllocationId, AllocationStatus};
    use crate::server::autoalloc::{AutoAllocError, AutoAllocResult, AutoAllocState};
    use async_trait::async_trait;
    use std::time::Duration;

    #[test]
    fn test_add_descriptor_with_same_name_twice() {
        let mut state = AutoAllocState::new(Duration::from_secs(1));

        #[async_trait(?Send)]
        impl QueueDescriptor for () {
            fn target_scale(&self) -> u64 {
                0
            }

            async fn schedule_allocation(
                &self,
                worker_count: u64,
            ) -> AutoAllocResult<AllocationId> {
                todo!()
            }

            async fn get_allocation_status(
                &self,
                allocation_id: &AllocationId,
            ) -> AutoAllocResult<Option<AllocationStatus>> {
                todo!()
            }
        }

        let name = "foo".to_string();
        assert!(state.add_descriptor(name.clone(), Box::new(())).is_ok());
        assert!(matches!(
            state.add_descriptor(name, Box::new(())),
            Err(AutoAllocError::DescriptorAlreadyExists(_))
        ));
    }
}
