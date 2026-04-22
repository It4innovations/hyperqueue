use crate::internal::common::resources::ResourceId;
use crate::internal::server::workerload::WorkerResources;
use crate::internal::solver::{ConstraintType, LpSolver};
use crate::resources::{ResourceAmount, ResourceRequestVariants, ResourceRqId, ResourceRqMap};
use crate::{Map, ResourceVariantId};
use hashbrown::Equivalent;
use std::cell::RefCell;

#[derive(Default)]
pub(crate) struct GapCache {
    inner: RefCell<GapCacheInner>,
}

#[derive(Hash, PartialEq, Eq)]
struct GapKey {
    rq: ResourceRqId,
    resources: WorkerResources,
}

#[derive(Hash, PartialEq, Eq)]
struct GapKeyRef<'a> {
    rq: ResourceRqId,
    resources: &'a WorkerResources,
}

impl<'a> Equivalent<GapKey> for GapKeyRef<'a> {
    fn equivalent(&self, key: &GapKey) -> bool {
        self.rq == key.rq && self.resources == &key.resources
    }
}

#[derive(Default)]
struct GapCacheInner {
    resource_gaps: Map<GapKey, WorkerResources>,
}

impl GapCache {
    pub fn get_gap(
        &self,
        high_priority_rq: ResourceRqId,
        low_priority_rq: ResourceRqId,
        resources: &WorkerResources,
        assigned_tasks: impl Iterator<Item = (ResourceRqId, ResourceVariantId)>,
        resource_rq_map: &ResourceRqMap,
    ) -> u32 {
        let h_rqv = resource_rq_map.get(high_priority_rq);
        if h_rqv.is_multi_node() {
            return 0;
        }
        let l_rqv = resource_rq_map.get(low_priority_rq);
        if l_rqv.is_multi_node() {
            return 0;
        }
        let mut free: WorkerResources = if let Some(h_rq) = h_rqv.trivial_request() {
            if h_rq.entries().iter().any(|r| r.request.amount_is_all()) {
                return 0;
            }
            let count = resources.task_max_count_for_request(h_rq);
            let mut resources = resources.clone();
            resources.remove_multiple(h_rq, count);
            resources
        } else {
            let key = GapKeyRef {
                rq: high_priority_rq,
                resources,
            };
            if let Some(free) = self.inner.borrow().resource_gaps.get(&key) {
                free.clone()
            } else {
                let free = compute_gap_resources(h_rqv, resources);
                self.inner.borrow_mut().resource_gaps.insert(
                    GapKey {
                        rq: high_priority_rq,
                        resources: resources.clone(),
                    },
                    free.clone(),
                );
                free
            }
        };
        for (rq_id, rv_id) in assigned_tasks {
            if rq_id != high_priority_rq {
                let rq = resource_rq_map.get(rq_id).get(rv_id);
                free.remove(rq);
            }
        }
        l_rqv
            .requests()
            .iter()
            .map(|rq| free.task_max_count_for_request(rq))
            .min()
            .unwrap_or(0)
    }
}

fn compute_gap_resources(
    rqv: &ResourceRequestVariants,
    resources: &WorkerResources,
) -> WorkerResources {
    let Some(n_unresources) = rqv
        .requests()
        .iter()
        .flat_map(|rq| rq.entries().iter().map(|r| r.resource_id.as_usize()))
        .max()
    else {
        return WorkerResources::new(Vec::new().into());
    };
    let n_resources = n_unresources + 1;
    let gap_res: Vec<ResourceAmount> = resources
        .iter_pairs()
        .map(|(r_id, r_amount)| {
            let mut solver = LpSolver::new(false);
            let mut cst = vec![Vec::new(); n_resources];
            let vars: Vec<_> = rqv
                .requests()
                .iter()
                .map(|rq| {
                    let a = rq.get_amount(r_id).unwrap_or(resources.get(r_id)).as_f64();
                    solver.add_nat_variable(a)
                })
                .collect();
            for (i, rq) in rqv.requests().iter().enumerate() {
                for entry in rq.entries() {
                    let a = entry
                        .request
                        .amount_or_none_if_all()
                        .unwrap_or(resources.get(r_id))
                        .as_f64();
                    cst[entry.resource_id.as_usize()].push((vars[i], a));
                }
            }
            for (idx, c) in cst.into_iter().enumerate() {
                let r_id = ResourceId::new(idx as u32);
                solver.add_constraint(
                    ConstraintType::Max,
                    resources.get(r_id).as_f64(),
                    c.into_iter(),
                );
            }
            let Some((_, v)) = solver.solve() else {
                return ResourceAmount::ZERO;
            };
            r_amount - ResourceAmount::from_float(v.round() as f32)
        })
        .collect();
    WorkerResources::new(gap_res.into())
}

#[cfg(test)]
mod tests {
    use crate::internal::server::core::CoreSplitMut;
    use std::iter;

    use crate::tests::utils::env::TestEnv;
    use crate::tests::utils::task::TaskBuilder;
    use crate::tests::utils::worker::WorkerBuilder;
    use crate::{TaskId, WorkerId};

    fn compute_gap(rt: &mut TestEnv, high_task: TaskId, low_task: TaskId, w: WorkerId) -> u32 {
        let CoreSplitMut {
            task_map,
            worker_map,
            scheduler_state,
            request_map,
            ..
        } = rt.core().split_mut();
        let h_rq = task_map.get_task(high_task).resource_rq_id;
        let l_rq = task_map.get_task(low_task).resource_rq_id;
        let res = &worker_map.get_worker(w).resources;
        scheduler_state
            .gap_cache
            .get_gap(h_rq, l_rq, &res, iter::empty(), request_map)
    }

    #[test]
    fn test_compute_gap() {
        let mut rt = TestEnv::new();
        rt.new_named_resource("foo");
        rt.new_named_resource("bar");
        let w = rt.new_worker(&WorkerBuilder::new(4));
        let t1 = rt.new_task_cpus(2);
        let t2 = rt.new_task_cpus(1);
        assert_eq!(compute_gap(&mut rt, t1, t2, w), 0);
        let t1 = rt.new_task_cpus(3);
        assert_eq!(compute_gap(&mut rt, t1, t2, w), 1);
        let t2 = rt.new_task_cpus(2);
        assert_eq!(compute_gap(&mut rt, t1, t2, w), 0);

        let w = rt.new_worker(&WorkerBuilder::new(12).res_sum("foo", 2).res_sum("bar", 1));
        let t1 = rt.new_task_cpus(4);
        let t2 = rt.new_task_cpus(2);
        assert_eq!(compute_gap(&mut rt, t1, t2, w), 0);
        let t1 = rt.new_task_cpus(5);
        let t2 = rt.new_task_cpus(1);
        assert_eq!(compute_gap(&mut rt, t1, t2, w), 2);
        let t1 = rt.new_task(&TaskBuilder::new().cpus(5).add_resource(1, 2));
        assert_eq!(compute_gap(&mut rt, t1, t2, w), 7);
        let t2 = rt.new_task(&TaskBuilder::new().cpus(1).add_resource(1, 1));
        assert_eq!(compute_gap(&mut rt, t1, t2, w), 0);
        let t1 = rt.new_task(&TaskBuilder::new().cpus(5).add_resource(1, 2));
        let t2 = rt.new_task(&TaskBuilder::new().cpus(1).add_resource(2, 1));
        assert_eq!(compute_gap(&mut rt, t1, t2, w), 1);
        let t1 = rt.new_task(
            &TaskBuilder::new()
                .cpus(8)
                .next_variant()
                .cpus(2)
                .add_resource(1, 2),
        );
        let t2 = rt.new_task(&TaskBuilder::new().cpus(1));
        assert_eq!(compute_gap(&mut rt, t1, t2, w), 2);
        let t1 = rt.new_task(
            &TaskBuilder::new()
                .cpus(8)
                .next_variant()
                .cpus(2)
                .add_resource(1, 1),
        );
        assert_eq!(compute_gap(&mut rt, t1, t2, w), 0);
        let t1 = rt.new_task(
            &TaskBuilder::new()
                .cpus(8)
                .next_variant()
                .cpus(2)
                .add_resource(1, 2),
        );
        let t2 = rt.new_task(&TaskBuilder::new().cpus(1).add_resource(2, 1));
        assert_eq!(compute_gap(&mut rt, t1, t2, w), 1);

        let w = rt.new_worker(&WorkerBuilder::new(6).res_sum("foo", 2).res_sum("bar", 2));
        let t1 = rt.new_task(
            &TaskBuilder::new()
                .cpus(2)
                .add_resource(1, 1)
                .next_variant()
                .cpus(2)
                .add_resource(2, 1),
        );
        let t2 = rt.new_task_cpus(1);
        assert_eq!(compute_gap(&mut rt, t1, t2, w), 0);

        let w = rt.new_worker(&WorkerBuilder::new(58));
        let t1 = rt.new_task(&TaskBuilder::new().cpus(13).next_variant().cpus(7));
        let t2 = rt.new_task_cpus(1);
        assert_eq!(compute_gap(&mut rt, t1, t2, w), 2);
    }
}
