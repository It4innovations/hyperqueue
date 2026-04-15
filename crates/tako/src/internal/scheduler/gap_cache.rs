use crate::internal::common::resources::ResourceId;
use crate::internal::server::workerload::WorkerResources;
use crate::internal::solver::{ConstraintType, LpSolution, LpSolver};
use crate::resources::{ResourceRequest, ResourceRequestVariants, ResourceRqId, ResourceRqMap};
use crate::{Map, ResourceVariantId};
use hashbrown::Equivalent;
use std::cell::RefCell;

#[derive(Default)]
pub(crate) struct GapCache {
    inner: RefCell<GapCacheInner>,
}

#[derive(Hash, PartialEq, Eq)]
struct GapKey {
    high_priority_rq: ResourceRqId,
    low_priority_rq: ResourceRqId,
    low_priority_variant: ResourceVariantId,
    resources: WorkerResources,
}

#[derive(Hash, PartialEq, Eq)]
struct GapKeyRef<'a> {
    high_priority_rq: ResourceRqId,
    low_priority_rq: ResourceRqId,
    low_priority_variant: ResourceVariantId,
    resources: &'a WorkerResources,
}

impl<'a> Equivalent<GapKey> for GapKeyRef<'a> {
    fn equivalent(&self, key: &GapKey) -> bool {
        self.high_priority_rq == key.high_priority_rq
            && self.low_priority_rq == key.low_priority_rq
            && self.resources == &key.resources
    }
}

#[derive(Default)]
struct GapCacheInner {
    resource_gaps: Map<GapKey, u32>,
}

impl GapCache {
    pub fn get_gap(
        &self,
        high_priority_rq: ResourceRqId,
        low_priority_rq: ResourceRqId,
        low_priority_variant: ResourceVariantId,
        resources: &WorkerResources,
        resource_rq_map: &ResourceRqMap,
    ) -> u32 {
        let key = GapKeyRef {
            high_priority_rq,
            low_priority_rq,
            low_priority_variant,
            resources,
        };
        let mut inner = self.inner.borrow_mut();

        if let Some(gap) = inner.resource_gaps.get(&key).copied() {
            gap
        } else {
            let gap = compute_gap(
                resource_rq_map.get(high_priority_rq),
                resource_rq_map
                    .get(low_priority_rq)
                    .get(low_priority_variant),
                resources,
            );
            inner.resource_gaps.insert(
                GapKey {
                    high_priority_rq,
                    low_priority_rq,
                    low_priority_variant,
                    resources: resources.clone(),
                },
                gap,
            );
            gap
        }
    }
}

fn compute_gap(
    high_priority_rqv: &ResourceRequestVariants,
    low_priority_rq: &ResourceRequest,
    resources: &WorkerResources,
) -> u32 {
    if high_priority_rqv.is_multi_node() || low_priority_rq.is_multi_node() {
        return 0;
    }
    if high_priority_rqv.is_trivial() {
        let high_priority_rq = high_priority_rqv.get(0.into());
        let count = resources.task_max_count_for_request(high_priority_rq);
        let mut resources = resources.clone();
        resources.remove_multiple(high_priority_rq, count);
        resources.task_max_count_for_request(low_priority_rq)
    } else {
        if high_priority_rqv
            .requests()
            .iter()
            .any(|rq| rq.entries().iter().any(|r| r.request.amount_is_all()))
        {
            return 0;
        }
        low_priority_rq
            .entries()
            .iter()
            .map(|entry| {
                let mut solver = LpSolver::new(false);
                let vars: Vec<_> = high_priority_rqv
                    .requests()
                    .iter()
                    .map(|rq| {
                        solver.add_nat_variable(rq.get_amount(entry.resource_id).unwrap().as_f64())
                    })
                    .collect();
                let max_resource: usize = high_priority_rqv
                    .requests()
                    .iter()
                    .flat_map(|rq| rq.entries().iter().map(|r| r.resource_id.as_usize()))
                    .max()
                    .unwrap_or(0);
                let mut cst = vec![Vec::new(); max_resource + 1];
                for (i, rq) in high_priority_rqv.requests().iter().enumerate() {
                    for entry in rq.entries() {
                        cst[entry.resource_id.as_usize()].push((
                            vars[i],
                            entry.request.amount_or_none_if_all().unwrap().as_f64(),
                        ));
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
                let Some((solution, _)) = solver.solve() else {
                    return 0;
                };
                let mut resources = resources.clone();
                for (var, rq) in vars.iter().zip(high_priority_rqv.requests().iter()) {
                    resources.remove_multiple_masked(
                        rq,
                        solution.get_value(*var).round() as u32,
                        entry.resource_id,
                    );
                }
                resources.task_max_count_for_request(low_priority_rq)
            })
            .min()
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::tests::utils::env::TestEnv;
    use crate::tests::utils::resources::ResBuilder;

    use crate::tests::utils::worker::WorkerBuilder;

    #[test]
    fn test_compute_gap() {
        let mut rt = TestEnv::new();
        rt.new_named_resource("foo");
        rt.new_named_resource("bar");
        let w = rt.new_worker(&WorkerBuilder::new(4));
        let rqv1 = ResBuilder::default().cpus(2).finish_v();
        let rq2 = ResBuilder::default().cpus(1).finish();
        assert_eq!(compute_gap(&rqv1, &rq2, &rt.worker(w).resources), 0);
        let rqv1 = ResBuilder::default().cpus(3).finish_v();
        let rq2 = ResBuilder::default().cpus(1).finish();
        assert_eq!(compute_gap(&rqv1, &rq2, &rt.worker(w).resources), 1);
        let rqv1 = ResBuilder::default().cpus(3).finish_v();
        let rq2 = ResBuilder::default().cpus(2).finish();
        assert_eq!(compute_gap(&rqv1, &rq2, &rt.worker(w).resources), 0);

        let w = rt.new_worker(&WorkerBuilder::new(12).res_sum("foo", 2).res_sum("bar", 1));
        let rqv1 = ResBuilder::default().cpus(4).finish_v();
        let rq2 = ResBuilder::default().cpus(2).finish();
        assert_eq!(compute_gap(&rqv1, &rq2, &rt.worker(w).resources), 0);
        let rqv1 = ResBuilder::default().cpus(5).finish_v();
        let rq2 = ResBuilder::default().cpus(1).finish();
        assert_eq!(compute_gap(&rqv1, &rq2, &rt.worker(w).resources), 2);
        let rqv1 = ResBuilder::default().cpus(5).add_compact(1, 2).finish_v();
        let rq2 = ResBuilder::default().cpus(1).finish();
        assert_eq!(compute_gap(&rqv1, &rq2, &rt.worker(w).resources), 7);
        let rqv1 = ResBuilder::default().cpus(5).add_compact(1, 2).finish_v();
        let rq2 = ResBuilder::default().cpus(1).add_compact(1, 1).finish();
        assert_eq!(compute_gap(&rqv1, &rq2, &rt.worker(w).resources), 0);
        let rqv1 = ResBuilder::default().cpus(5).add_compact(1, 2).finish_v();
        let rq2 = ResBuilder::default().cpus(1).add_compact(2, 1).finish();
        assert_eq!(compute_gap(&rqv1, &rq2, &rt.worker(w).resources), 1);

        let mut rqv1 = ResBuilder::default().cpus(8).finish_v();
        rqv1.add_varint(ResBuilder::default().cpus(2).add_compact(1, 2));
        let rq2 = ResBuilder::default().cpus(1).finish();
        assert_eq!(compute_gap(&rqv1, &rq2, &rt.worker(w).resources), 2);

        let mut rqv1 = ResBuilder::default().cpus(8).finish_v();
        rqv1.add_varint(ResBuilder::default().cpus(2).add_compact(1, 1));
        let rq2 = ResBuilder::default().cpus(1).finish();
        assert_eq!(compute_gap(&rqv1, &rq2, &rt.worker(w).resources), 0);

        let mut rqv1 = ResBuilder::default().cpus(8).finish_v();
        rqv1.add_varint(ResBuilder::default().cpus(2).add_compact(1, 2));
        let rq2 = ResBuilder::default().cpus(1).add_compact(2, 1).finish();
        assert_eq!(compute_gap(&rqv1, &rq2, &rt.worker(w).resources), 1);

        let w = rt.new_worker(&WorkerBuilder::new(6).res_sum("foo", 2).res_sum("bar", 2));
        let mut rqv1 = ResBuilder::default().cpus(2).add_compact(1, 1).finish_v();
        rqv1.add_varint(ResBuilder::default().cpus(2).add_compact(2, 1));
        let rq2 = ResBuilder::default().cpus(1).finish();
        assert_eq!(compute_gap(&rqv1, &rq2, &rt.worker(w).resources), 0);

        let w = rt.new_worker(&WorkerBuilder::new(58));
        let mut rqv1 = ResBuilder::default().cpus(13).finish_v();
        rqv1.add_varint(ResBuilder::default().cpus(7));
        let rq2 = ResBuilder::default().cpus(1).finish();
        assert_eq!(compute_gap(&rqv1, &rq2, &rt.worker(w).resources), 2);
    }
}
