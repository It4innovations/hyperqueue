use std::time::Instant;

use crate::server::autoalloc::state::{Allocation, AllocationStatus, DescriptorState};
use crate::server::state::StateRef;

// We assume that this process will not be running concurrently
#[allow(clippy::await_holding_refcell_ref)]
pub async fn autoalloc_check(state_ref: &StateRef) {
    log::debug!("Running autoalloc");

    let autoalloc_ref = state_ref.get().get_autoalloc_state().clone();
    let mut autoalloc = autoalloc_ref.get_mut();
    for descriptor in autoalloc.descriptors() {
        process_descriptor(descriptor).await;
    }
}

async fn process_descriptor(state: &mut DescriptorState) {
    let scale = state.descriptor.target_scale();

    // TODO: check only once in a while
    refresh_allocations(state).await;
    schedule_new_allocations(state, scale).await
}

async fn refresh_allocations(state: &mut DescriptorState) {
    let mut next_allocations = Vec::with_capacity(state.allocations.len());
    for allocation in std::mem::take(&mut state.allocations) {
        // TODO: handle errors
        let result = state
            .descriptor
            .get_allocation_status(&allocation.id)
            .await
            .unwrap();
        if let Some(status) = result {
            next_allocations.push(Allocation {
                id: allocation.id,
                worker_count: allocation.worker_count,
                status,
            });
        }
    }
    state.allocations = next_allocations;
}

async fn schedule_new_allocations(state: &mut DescriptorState, scale: u64) {
    let active_workers = state
        .allocations
        .iter()
        .map(|alloc| alloc.worker_count)
        .sum();
    let mut remaining = scale.saturating_sub(active_workers);
    while remaining > 0 {
        let to_schedule = std::cmp::min(remaining, state.descriptor.max_workers_per_alloc());
        // TODO: handle errors
        let result = state
            .descriptor
            .schedule_allocation(to_schedule)
            .await
            .unwrap();
        state.allocations.push(Allocation {
            id: result,
            worker_count: to_schedule,
            status: AllocationStatus::Queued {
                queued_at: Instant::now(),
            },
        });

        remaining -= to_schedule;
    }
}

pub async fn autoalloc_process(state_ref: StateRef) {
    let duration = state_ref
        .get()
        .get_autoalloc_state()
        .get()
        .refresh_interval();
    let mut interval = tokio::time::interval(duration);
    loop {
        interval.tick().await;
        autoalloc_check(&state_ref).await;
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::time::{Duration, Instant};

    use async_trait::async_trait;

    use crate::common::WrappedRcRefCell;
    use crate::server::autoalloc::descriptor::QueueDescriptor;
    use crate::server::autoalloc::process::autoalloc_check;
    use crate::server::autoalloc::state::{AllocationId, AllocationStatus};
    use crate::server::autoalloc::AutoAllocResult;
    use crate::server::state::StateRef;

    #[tokio::test]
    async fn test_do_not_overallocate_queue() {
        let state = create_state();

        let call_count = WrappedRcRefCell::wrap(0);

        add_autoalloc_descriptor(
            &state,
            call_count.clone(),
            move |s, worker_count| async move {
                *s.get_mut() += 1;
                Ok("1".to_string())
            },
            move |_, _| async move {
                Ok(Some(AllocationStatus::Queued {
                    queued_at: Instant::now(),
                }))
            },
            1,
            1,
        )
        .await;

        autoalloc_check(&state).await;
        autoalloc_check(&state).await;

        assert_eq!(*call_count.get(), 1);
    }

    #[tokio::test]
    async fn test_split_allocations_per_max_job_size() {
        let state = create_state();

        struct State {
            requests: Vec<u64>,
        }

        let call_count = WrappedRcRefCell::wrap(State {
            requests: vec![3, 3, 3, 1],
        });

        add_autoalloc_descriptor(
            &state,
            call_count.clone(),
            move |s, worker_count| async move {
                let mut state = s.get_mut();
                assert_eq!(worker_count, state.requests[0]);
                state.requests.remove(0);
                Ok(state.requests.len().to_string())
            },
            move |_, _| async move {
                Ok(Some(AllocationStatus::Queued {
                    queued_at: Instant::now(),
                }))
            },
            10,
            3,
        )
        .await;

        autoalloc_check(&state).await;

        assert_eq!(call_count.get().requests.len(), 0);
    }

    async fn add_autoalloc_descriptor<
        State: 'static,
        ScheduleFn: 'static + Fn(WrappedRcRefCell<State>, u64) -> ScheduleFnFut,
        ScheduleFnFut: Future<Output = AutoAllocResult<AllocationId>>,
        StatusFn: 'static + Fn(WrappedRcRefCell<State>, &AllocationId) -> StatusFnFut,
        StatusFnFut: Future<Output = AutoAllocResult<Option<AllocationStatus>>>,
    >(
        state_ref: &StateRef,
        custom_state: WrappedRcRefCell<State>,
        schedule_fn: ScheduleFn,
        status_fn: StatusFn,
        target_scale: u64,
        max_workers_per_alloc: u64,
    ) {
        struct Queue<ScheduleFn, StatusFn, State> {
            target_scale: u64,
            max_workers_per_alloc: u64,
            schedule_fn: ScheduleFn,
            status_fn: StatusFn,
            custom_state: WrappedRcRefCell<State>,
        }

        #[async_trait(?Send)]
        impl<
                State: 'static,
                ScheduleFn: 'static + Fn(WrappedRcRefCell<State>, u64) -> ScheduleFnFut,
                ScheduleFnFut: Future<Output = AutoAllocResult<AllocationId>>,
                StatusFn: 'static + Fn(WrappedRcRefCell<State>, &AllocationId) -> StatusFnFut,
                StatusFnFut: Future<Output = AutoAllocResult<Option<AllocationStatus>>>,
            > QueueDescriptor for Queue<ScheduleFn, StatusFn, State>
        {
            fn target_scale(&self) -> u64 {
                self.target_scale
            }

            fn max_workers_per_alloc(&self) -> u64 {
                self.max_workers_per_alloc
            }

            async fn schedule_allocation(
                &self,
                worker_count: u64,
            ) -> AutoAllocResult<AllocationId> {
                (self.schedule_fn)(self.custom_state.clone(), worker_count).await
            }

            async fn get_allocation_status(
                &self,
                allocation_id: &AllocationId,
            ) -> AutoAllocResult<Option<AllocationStatus>> {
                (self.status_fn)(self.custom_state.clone(), allocation_id).await
            }
        }

        let queue = Queue {
            target_scale,
            max_workers_per_alloc,
            schedule_fn,
            status_fn,
            custom_state,
        };

        state_ref
            .get()
            .get_autoalloc_state()
            .get_mut()
            .add_descriptor("foo".to_string(), Box::new(queue))
            .unwrap();
    }

    fn create_state() -> StateRef {
        StateRef::new(Duration::from_millis(100))
    }
}
