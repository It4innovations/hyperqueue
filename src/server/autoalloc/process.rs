use crate::common::WrappedRcRefCell;
use crate::server::autoalloc::state::{Allocation, AllocationEvent, AllocationStatus};
use crate::server::autoalloc::AutoAllocState;
use crate::server::state::StateRef;
use std::time::Instant;

macro_rules! get_or_return {
    ($e:expr) => {
        match $e {
            Some(v) => v,
            _ => return,
        }
    };
}

pub async fn autoalloc_check(state_ref: &StateRef) {
    log::debug!("Running autoalloc");

    let autoalloc_ref = state_ref.get().get_autoalloc_state().clone();
    let descriptors: Vec<_> = autoalloc_ref
        .get()
        .descriptor_names()
        .map(|v| v.to_string())
        .collect();

    for name in descriptors {
        process_descriptor(&name, &autoalloc_ref).await;
    }
}

async fn process_descriptor(name: &str, state: &WrappedRcRefCell<AutoAllocState>) {
    // TODO: check only once in a while
    refresh_allocations(name, state).await;
    schedule_new_allocations(name, state).await
}

#[allow(clippy::await_holding_refcell_ref)]
async fn refresh_allocations(name: &str, state_ref: &WrappedRcRefCell<AutoAllocState>) {
    let allocations = {
        let mut state = state_ref.get_mut();
        let descriptor_state = get_or_return!(state.get_descriptor_mut(name));

        let next_allocations = Vec::with_capacity(descriptor_state.allocations.len());
        std::mem::replace(&mut descriptor_state.allocations, next_allocations)
    };
    for allocation in allocations {
        let descriptor = get_or_return!(state_ref.get().get_descriptor(name))
            .descriptor
            .clone();

        let result = descriptor.get().get_allocation_status(&allocation.id).await;

        let mut state = state_ref.get_mut();
        let descriptor = get_or_return!(state.get_descriptor_mut(name));
        match result {
            Ok(status) => {
                if let Some(status) = status {
                    descriptor.allocations.push(Allocation {
                        id: allocation.id,
                        worker_count: allocation.worker_count,
                        status,
                    });
                } else {
                    descriptor.add_event(AllocationEvent::Finished(allocation.id));
                }
            }
            Err(err) => {
                log::error!(
                    "Failed to get allocation {} status from {}: {}",
                    allocation.id,
                    name,
                    err
                );
                descriptor.add_event(AllocationEvent::StatusFail(err));
            }
        }
    }
}

#[allow(clippy::await_holding_refcell_ref)]
async fn schedule_new_allocations(name: &str, state_ref: &WrappedRcRefCell<AutoAllocState>) {
    let (mut remaining, max_workers_per_alloc) = {
        let state = state_ref.get();
        let descriptor = get_or_return!(state.get_descriptor(name));
        let active_workers = descriptor
            .allocations
            .iter()
            .map(|alloc| alloc.worker_count)
            .sum();

        let descriptor_impl = descriptor.descriptor.get();
        let scale = descriptor_impl.target_scale();
        (
            scale.saturating_sub(active_workers),
            descriptor_impl.max_workers_per_alloc(),
        )
    };
    while remaining > 0 {
        let to_schedule = std::cmp::min(remaining, max_workers_per_alloc);
        let descriptor = get_or_return!(state_ref.get().get_descriptor(name))
            .descriptor
            .clone();

        let result = descriptor.get().schedule_allocation(to_schedule).await;

        let mut state = state_ref.get_mut();
        let descriptor = get_or_return!(state.get_descriptor_mut(name));
        match result {
            Ok(id) => {
                log::info!("Queued {} workers into {}", to_schedule, name);
                descriptor.add_event(AllocationEvent::QueueSuccess(id.clone()));
                descriptor.allocations.push(Allocation {
                    id,
                    worker_count: to_schedule,
                    status: AllocationStatus::Queued {
                        queued_at: Instant::now(),
                    },
                });
            }
            Err(err) => {
                log::error!("Failed to queue allocation into {}: {}", name, err);
                descriptor.add_event(AllocationEvent::QueueFail(err));
            }
        }

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
    use crate::server::autoalloc::state::{AllocationEvent, AllocationId, AllocationStatus};
    use crate::server::autoalloc::{AutoAllocError, AutoAllocResult};
    use crate::server::state::StateRef;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[tokio::test]
    async fn test_do_not_overallocate_queue() {
        let state = create_state();
        let call_count = WrappedRcRefCell::wrap(0);

        add_descriptor(
            &state,
            call_count.clone(),
            move |s, _| async move {
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

        add_descriptor(
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

    #[tokio::test]
    async fn test_log_failed_allocation_attempt() {
        let state = create_state();

        add_descriptor(
            &state,
            WrappedRcRefCell::wrap(()),
            move |_, _| async move { Err(AutoAllocError::Custom("foo".to_string())) },
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

        let state = state.get();
        let state = state.get_autoalloc_state().get();
        let descriptor = state.get_descriptor("foo").unwrap();
        let event = descriptor.get_events()[0].clone();
        matches!(event.event, AllocationEvent::QueueFail(_));
    }

    #[tokio::test]
    async fn test_reschedule_after_job_ends() {
        let state = create_state();

        #[derive(Default)]
        struct State {
            job_id: u64,
            status: Option<AllocationStatus>,
        }

        let custom_state = WrappedRcRefCell::wrap(State::default());

        add_descriptor(
            &state,
            custom_state.clone(),
            move |s, _| async move {
                s.get_mut().job_id += 1;
                Ok(s.get().job_id.to_string())
            },
            move |s, _| async move { Ok(s.get().status.clone()) },
            1,
            1,
        )
        .await;

        autoalloc_check(&state).await;
        custom_state.get_mut().status = Some(AllocationStatus::Queued {
            queued_at: Instant::now(),
        });
        autoalloc_check(&state).await;
        custom_state.get_mut().status = None;
        autoalloc_check(&state).await;

        assert_eq!(custom_state.get().job_id, 2);
    }

    async fn add_descriptor<
        State: 'static,
        ScheduleFn: 'static + Fn(WrappedRcRefCell<State>, u64) -> ScheduleFnFut,
        ScheduleFnFut: Future<Output = AutoAllocResult<AllocationId>>,
        StatusFn: 'static + Fn(WrappedRcRefCell<State>, &str) -> StatusFnFut,
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
                StatusFn: 'static + Fn(WrappedRcRefCell<State>, &str) -> StatusFnFut,
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
                allocation_id: &str,
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
            .add_descriptor(
                "foo".to_string(),
                WrappedRcRefCell::new_wrapped(Rc::new(RefCell::new(queue))),
            )
            .unwrap();
    }

    fn create_state() -> StateRef {
        StateRef::new(Duration::from_millis(100))
    }
}
