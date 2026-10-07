use crate::internal::common::resources::ResourceId;
use crate::internal::server::workerload::WorkerResources;
use crate::internal::solver::{ConstraintType, LpSolution, LpSolver};
use crate::resources::{
    ResourceAmount, ResourceRequest, ResourceRequestVariants, ResourceRqId, ResourceRqMap,
};
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
    #[cfg(test)]
    pub fn get_gap(
        &self,
        high_priority_rq: ResourceRqId,
        low_priority_rq: ResourceRqId,
        resources: &WorkerResources,
        assigned_tasks: impl Iterator<Item = (ResourceRqId, ResourceVariantId)>,
        resource_rq_map: &ResourceRqMap,
    ) -> u32 {
        let l_rqv = resource_rq_map.get(low_priority_rq);
        if l_rqv.is_multi_node() {
            return 0;
        }
        let Some(free) =
            self.gap_resources(high_priority_rq, resources, assigned_tasks, resource_rq_map)
        else {
            return 0;
        };
        l_rqv
            .requests()
            .iter()
            .map(|rq| free.task_max_count_for_request(rq))
            .min()
            .unwrap_or(0)
    }

    /// The gap allowance of a worker for a blocker: resources that tasks of any lower request may
    /// jointly consume there without ever denying the blocker (`get_gap` counts how many tasks of
    /// one request fit into it). `None` when no gap exists: multi-node blockers, and blockers that
    /// ask for all of a resource.
    pub fn gap_resources(
        &self,
        high_priority_rq: ResourceRqId,
        resources: &WorkerResources,
        assigned_tasks: impl Iterator<Item = (ResourceRqId, ResourceVariantId)>,
        resource_rq_map: &ResourceRqMap,
    ) -> Option<WorkerResources> {
        let h_rqv = resource_rq_map.get(high_priority_rq);
        if h_rqv.is_multi_node() {
            return None;
        }
        // Callers ask only for workers that can run the blocker; otherwise the gap is meaningless
        // and the blocker may name a resource the worker lacks (out of bounds in `remove_multiple`).
        debug_assert!(
            h_rqv
                .requests()
                .iter()
                .any(|rq| resources.is_capable_to_run_request(rq)),
            "gap computed for a worker that cannot run the blocker"
        );
        let mut free: WorkerResources = if let Some(h_rq) = h_rqv.trivial_request() {
            if h_rq.entries().iter().any(|r| r.request.amount_is_all()) {
                return None;
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
        let occupants: Vec<&ResourceRequest> = assigned_tasks
            .filter(|(rq_id, _)| *rq_id != high_priority_rq)
            .map(|(rq_id, rv_id)| resource_rq_map.get(rq_id).get(rv_id))
            .collect();
        for rq in &occupants {
            free.remove(rq);
        }
        if let Some((resource_id, exact)) = exact_single_resource_gap(h_rqv, resources, &occupants)
        {
            free.set(resource_id, exact);
        } else if let Some(exact) = exact_gap_by_enumeration(h_rqv, resources, &occupants) {
            free = exact;
        }
        Some(free)
    }
}

const MAX_EXACT_GAP_UNITS: u32 = 4096;

fn exact_single_resource_gap(
    h_rqv: &ResourceRequestVariants,
    resources: &WorkerResources,
    occupants: &[&ResourceRequest],
) -> Option<(ResourceId, ResourceAmount)> {
    let h_rq = h_rqv.trivial_request()?;
    let entries = h_rq.entries();
    if entries.len() != 1 {
        return None;
    }
    let entry = &entries[0];
    let blocker = entry.request.amount_or_none_if_all()?;
    let resource_id = entry.resource_id;
    let capacity = resources.get(resource_id);
    if blocker.fractions() != 0 || capacity.fractions() != 0 || blocker.units() == 0 {
        return None;
    }
    let r = blocker.units();
    if r > MAX_EXACT_GAP_UNITS {
        return None;
    }
    let capacity = capacity.units();

    let mut reachable = vec![false; r as usize];
    reachable[0] = true;
    for rq in occupants {
        let amount = rq.get_amount(resource_id).unwrap_or(ResourceAmount::ZERO);
        if amount.fractions() != 0 {
            return None;
        }
        let step = (amount.units() % r) as usize;
        if step == 0 {
            continue;
        }
        let previous = reachable.clone();
        for (value, _) in previous.iter().enumerate().filter(|(_, hit)| **hit) {
            reachable[(value + step) % r as usize] = true;
        }
    }

    let gap = (0..r)
        .filter(|v| reachable[*v as usize])
        .map(|v| (capacity + r - (v % r)) % r)
        .min()
        .unwrap_or(0);
    Some((resource_id, ResourceAmount::new(gap, 0)))
}

/// Capacity of one resource that no *mix* of `blockers` can use, whatever subset of the current
/// occupants departs.
///
/// The gap of `gap_resources` is computed for one blocker at a time, and is safe against that
/// blocker alone. Two blockers that pack better together than either of them does by itself can
/// still be delayed by tasks that respect both gaps separately: on a $12$-cpu worker with a
/// $5$-cpu and a $7$-cpu blocker the two gaps are $2$ and $5$, so two 1-cpu tasks admitted under
/// the first leave $10$ cpus, which no longer holds $5 + 7$. The value computed here bounds what
/// all gap users of a worker may take together, and for a single blocker it equals that blocker's
/// own gap, so nothing changes where only one request blocks.
///
/// Zero is always safe, and is the answer whenever the exact value is out of reach: fractional
/// amounts, or a capacity above `MAX_EXACT_GAP_UNITS`. A blocker that asks for *all* of the
/// resource is passed in as the whole capacity, which also yields zero.
pub(crate) fn joint_gap_units(
    capacity: ResourceAmount,
    blockers: &[(ResourceAmount, u32)],
    occupants: &[ResourceAmount],
) -> ResourceAmount {
    if capacity.fractions() != 0 || capacity.units() > MAX_EXACT_GAP_UNITS {
        return ResourceAmount::ZERO;
    }
    let capacity_units = capacity.units() as usize;
    // Each blocker is bounded by how many of its tasks the whole worker could hold: its other
    // resources limit the mix just as much as this one. Without that bound a blocker that needs
    // a scarce second resource would appear able to fill the worker on its own.
    let mut sizes: Vec<(usize, u32)> = Vec::with_capacity(blockers.len());
    for (amount, max_count) in blockers {
        if amount.fractions() != 0 {
            return ResourceAmount::ZERO;
        }
        if amount.units() > 0 && *max_count > 0 {
            sizes.push((amount.units() as usize, *max_count));
        }
    }
    if sizes.is_empty() {
        // No blocker consumes this resource, so none of it is being withheld from them.
        return capacity;
    }

    // `reachable[v]`: a mix of blockers, each within its own count, totals exactly `v`.
    let mut reachable = vec![false; capacity_units + 1];
    reachable[0] = true;
    for (size, max_count) in &sizes {
        let size = *size;
        if size.saturating_mul(*max_count as usize) >= capacity_units {
            // The count never binds here: the plain ascending pass is the unbounded case.
            for value in 0..=capacity_units.saturating_sub(size) {
                if reachable[value] {
                    reachable[value + size] = true;
                }
            }
            continue;
        }
        // Bounded: binary splitting turns `max_count` copies into `log(max_count)` items.
        let mut remaining = *max_count;
        let mut chunk = 1u32;
        while remaining > 0 {
            let take = chunk.min(remaining) as usize;
            let step = size * take;
            if step <= capacity_units {
                for value in (step..=capacity_units).rev() {
                    if reachable[value - step] {
                        reachable[value] = true;
                    }
                }
            }
            remaining -= chunk.min(remaining);
            chunk = chunk.saturating_mul(2);
        }
    }
    // `mix[v]`: the largest reachable total at or below `v`.
    let mut mix = vec![0usize; capacity_units + 1];
    for value in 1..=capacity_units {
        mix[value] = if reachable[value] {
            value
        } else {
            mix[value - 1]
        };
    }

    // Totals the current occupants can still hold once any subset of them has departed.
    let mut departed = vec![false; capacity_units + 1];
    departed[0] = true;
    for occupant in occupants {
        if occupant.fractions() != 0 {
            return ResourceAmount::ZERO;
        }
        let amount = occupant.units() as usize;
        if amount == 0 || amount > capacity_units {
            continue;
        }
        for value in (amount..=capacity_units).rev() {
            if departed[value - amount] {
                departed[value] = true;
            }
        }
    }

    let gap = (0..=capacity_units)
        .filter(|held| departed[*held])
        .map(|held| {
            let free = capacity_units - held;
            free - mix[free]
        })
        .min()
        .unwrap_or(0);
    ResourceAmount::new(gap as u32, 0)
}

const MAX_GAP_SUB_OCCUPANCIES: u32 = 2048;

fn exact_gap_by_enumeration(
    h_rqv: &ResourceRequestVariants,
    resources: &WorkerResources,
    occupants: &[&ResourceRequest],
) -> Option<WorkerResources> {
    let h_rq = h_rqv.trivial_request()?;
    if h_rq.entries().iter().any(|e| e.request.amount_is_all()) {
        return None;
    }
    let mut groups: Map<&ResourceRequest, u32> = Map::default();
    for rq in occupants {
        *groups.entry(*rq).or_default() += 1;
    }
    let groups = groups;
    let size = groups.values().try_fold(1u32, |acc, count| {
        acc.checked_mul(*count + 1)
            .filter(|size| *size <= MAX_GAP_SUB_OCCUPANCIES)
    })?;
    let mut gap: Option<WorkerResources> = None;
    let mut remaining_counts: Vec<u32> = groups.values().copied().collect();
    for _ in 0..size {
        let mut remaining = resources.clone();
        for ((rq, _), k) in groups.iter().zip(&remaining_counts) {
            if *k > 0 {
                remaining.remove_multiple(rq, *k);
            }
        }
        let fit = remaining.task_max_count_for_request(h_rq);
        remaining.remove_multiple(h_rq, fit);
        if let Some(g) = &mut gap {
            for (resource_id, amount) in remaining.iter_all_pairs() {
                if amount < g.get(resource_id) {
                    g.set(resource_id, amount);
                }
            }
        } else {
            if h_rq
                .entries()
                .iter()
                .all(|e| remaining.get(e.resource_id).is_zero())
            {
                return Some(remaining);
            }
            gap = Some(remaining);
        }
        for ((_, count), k) in groups.iter().zip(remaining_counts.iter_mut()) {
            if *k > 0 {
                *k -= 1;
                break;
            }
            *k = *count;
        }
    }
    gap
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
        .iter_all_pairs()
        .map(|(r_id, r_amount)| {
            if r_amount.is_zero() {
                return ResourceAmount::ZERO;
            }
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
            let Some(s) = solver.solve(None) else {
                return ResourceAmount::ZERO;
            };
            r_amount - ResourceAmount::from_float(LpSolution::objective(&s).round() as f32)
        })
        .collect();
    WorkerResources::new(gap_res.into())
}

#[cfg(test)]
mod tests {
    use crate::internal::server::core::CoreSplitMut;
    use crate::resources::{CPU_RESOURCE_ID, ResourceAmount};
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

    fn compute_gap_occupied(
        rt: &mut TestEnv,
        high_task: TaskId,
        low_task: TaskId,
        w: WorkerId,
        occupants: &[TaskId],
    ) -> u32 {
        let CoreSplitMut {
            task_map,
            worker_map,
            scheduler_state,
            request_map,
            ..
        } = rt.core().split_mut();
        let h_rq = task_map.get_task(high_task).resource_rq_id;
        let l_rq = task_map.get_task(low_task).resource_rq_id;
        let assigned: Vec<_> = occupants
            .iter()
            .map(|t| {
                let task = task_map.get_task(*t);
                (task.resource_rq_id, crate::ResourceVariantId::new(0))
            })
            .collect();
        let res = &worker_map.get_worker(w).resources;
        scheduler_state
            .gap_cache
            .get_gap(h_rq, l_rq, res, assigned.into_iter(), request_map)
    }

    #[test]
    fn test_gap_is_one() {
        let mut rt = TestEnv::new();
        let w = rt.new_worker(&WorkerBuilder::new(12));
        let r_h = rt.new_task_cpus(8);
        let r_l = rt.new_task_cpus(4);
        assert_eq!(compute_gap(&mut rt, r_h, r_l, w), 1, "empty worker");

        let occupant = rt.new_task_cpus(2);
        assert_eq!(
            compute_gap_occupied(&mut rt, r_h, r_l, w, &[occupant]),
            0,
            "two cpus occupied"
        );
    }

    #[test]
    fn test_gap_is_eight() {
        let mut rt = TestEnv::new();
        let gpu = rt.new_named_resource("gpus");
        let w = rt.new_worker(&WorkerBuilder::new(12).res_sum("gpus", 4));
        let r_h = rt.new_task(&TaskBuilder::new().cpus(1).add_resource(gpu, 1));
        let r_l = rt.new_task_cpus(1);
        assert_eq!(compute_gap(&mut rt, r_h, r_l, w), 8, "empty worker");

        let occupant = rt.new_task_cpus(2);
        assert_eq!(
            compute_gap_occupied(&mut rt, r_h, r_l, w, &[occupant]),
            6,
            "two cpus occupied"
        );
    }

    #[test]
    fn test_gap_is_safe_under_future_departures() {
        let mut rt = TestEnv::new();
        let w = rt.new_worker(&WorkerBuilder::new(12));
        let unrelated = rt.new_task_cpus(3);
        let r_h = rt.new_task_cpus(5);
        let r_l = rt.new_task_cpus(1);
        assert_eq!(
            compute_gap_occupied(&mut rt, r_h, r_l, w, &[unrelated]),
            2,
            "must be 12 mod 5 = 2, not the naive 9 - 5 = 4"
        );
    }

    /// The case that rules out the tempting shortcut of `min(gap(C), gap(C - occupancy))`.
    ///
    /// C = 12, r_h = 5, occupants {1, 4}: the occupancy still present at some future point can be
    /// 0, 1, 4 or 5, giving `(12 - x) mod 5` of 2, 1, 3, 2. The binding term is the *intermediate*
    /// x = 1, so the gap is 1 — both extremes give 2, and granting 2 would over-commit the worker.
    #[test]
    fn test_gap_binding_term_can_be_an_intermediate_sub_occupancy() {
        let mut rt = TestEnv::new();
        let w = rt.new_worker(&WorkerBuilder::new(12));
        let occ_a = rt.new_task_cpus(1);
        let occ_b = rt.new_task_cpus(4);
        let r_h = rt.new_task_cpus(5);
        let r_l = rt.new_task_cpus(1);
        assert_eq!(
            compute_gap_occupied(&mut rt, r_h, r_l, w, &[occ_a, occ_b]),
            1,
            "intermediate sub-occupancy must bind; 2 would over-grant"
        );
    }

    /// Cross-check the residue DP against brute-force enumeration of every sub-occupancy, over a
    /// spread of capacities, blocker widths and occupant multisets. The result must equal the
    /// exact minimum -- and in particular must never exceed it, which is the unsafe direction.
    #[test]
    fn test_gap_matches_brute_force_over_sub_occupancies() {
        for capacity in [6u32, 8, 11, 12, 16] {
            for blocker in [2u32, 3, 5, 6, 7] {
                if blocker > capacity {
                    // The gap is only computed for workers that can run the blocker
                    continue;
                }
                for occupants in [
                    vec![],
                    vec![1u32],
                    vec![3],
                    vec![1, 4],
                    vec![2, 2],
                    vec![1, 2, 4],
                    vec![5, 3, 1],
                ] {
                    if occupants.iter().sum::<u32>() > capacity {
                        continue;
                    }
                    // min over subsets that may remain occupied of (C - x) mod r
                    let mut expected = u32::MAX;
                    for mask in 0..(1u32 << occupants.len()) {
                        let x: u32 = occupants
                            .iter()
                            .enumerate()
                            .filter(|(i, _)| mask & (1 << i) != 0)
                            .map(|(_, a)| *a)
                            .sum();
                        expected = expected.min((capacity - x) % blocker);
                    }

                    let mut rt = TestEnv::new();
                    let w = rt.new_worker(&WorkerBuilder::new(capacity));
                    let occ: Vec<_> = occupants.iter().map(|a| rt.new_task_cpus(*a)).collect();
                    let r_h = rt.new_task_cpus(blocker);
                    let r_l = rt.new_task_cpus(1);
                    let got = compute_gap_occupied(&mut rt, r_h, r_l, w, &occ);
                    assert_eq!(
                        got, expected,
                        "C={capacity} r_h={blocker} occupants={occupants:?}"
                    );
                }
            }
        }
    }

    /// A blocker spanning two resources: the residue table does not apply, enumeration does.
    #[test]
    fn test_multi_resource_blocker_gap_is_exact() {
        let mut rt = TestEnv::new();
        let gpu = rt.new_named_resource("gpus");
        let w = rt.new_worker(&WorkerBuilder::new(12).res_sum("gpus", 4));
        let r_h = rt.new_task(&TaskBuilder::new().cpus(5).add_resource(gpu, 1));
        let r_l = rt.new_task_cpus(1);
        let occupant = rt.new_task_cpus(3);
        // Occupant running: 9 free cpus fit one blocker, 4 cpus it cannot use. Occupant finished:
        // 12 cpus fit two blockers, 2 it cannot use. Exact gap 2; the bound 12 - 10 - 3 gives 0.
        assert_eq!(compute_gap_occupied(&mut rt, r_h, r_l, w, &[occupant]), 2);
    }

    /// A blocker asking for a large amount of one resource is too big for the residue table, but
    /// with only two occupants there are four sub-occupancies, so the gap is still exact. This is
    /// the intermediate-sub-occupancy case scaled by 1000: exact 1000, where the conservative bound
    /// `C - M(C) - o` would give 0.
    #[test]
    fn test_large_single_resource_blocker_is_exact_by_enumeration() {
        let mut rt = TestEnv::new();
        let w = rt.new_worker(&WorkerBuilder::new(12_000));
        let occ_a = rt.new_task_cpus(1_000);
        let occ_b = rt.new_task_cpus(4_000);
        let r_h = rt.new_task_cpus(5_000);
        let r_l = rt.new_task_cpus(1);
        assert_eq!(
            compute_gap_occupied(&mut rt, r_h, r_l, w, &[occ_a, occ_b]),
            1_000
        );
    }

    /// Too large for the residue table and too many distinct sub-occupancies to enumerate: the
    /// conservative bound is used. The exact gap here would be 922; the bound is 0.
    #[test]
    fn test_gap_falls_back_to_bound_when_both_exact_methods_are_too_large() {
        let mut rt = TestEnv::new();
        let w = rt.new_worker(&WorkerBuilder::new(12_000));
        let mut occupants = vec![rt.new_task_cpus(1_000), rt.new_task_cpus(4_000)];
        // Twelve more distinct shapes: 2^14 sub-occupancies in total.
        occupants.extend((1..=12).map(|c| rt.new_task_cpus(c)));
        let r_h = rt.new_task_cpus(5_000);
        let r_l = rt.new_task_cpus(1);
        assert_eq!(compute_gap_occupied(&mut rt, r_h, r_l, w, &occupants), 0);
    }

    /// The per-resource minimum of `C - x - fit(C - x) * r_h` over every subset of occupants that
    /// may still be running, for a blocker of `blocker.0` cpus + `blocker.1` gpus. Occupants are
    /// `(cpus, gpus)`. Returns the allowance `(cpus, gpus)`.
    fn brute_force_gap(
        capacity: (u32, u32),
        blocker: (u32, u32),
        occupants: &[(u32, u32)],
    ) -> (u32, u32) {
        let mut gap = (u32::MAX, u32::MAX);
        for mask in 0..(1u32 << occupants.len()) {
            let (xc, xg) = occupants
                .iter()
                .enumerate()
                .filter(|(i, _)| mask & (1 << i) != 0)
                .fold((0, 0), |(c, g), (_, (oc, og))| (c + oc, g + og));
            let (fc, fg) = (capacity.0 - xc, capacity.1 - xg);
            let fit = (fc / blocker.0).min(fg / blocker.1);
            gap.0 = gap.0.min(fc - fit * blocker.0);
            gap.1 = gap.1.min(fg - fit * blocker.1);
        }
        gap
    }

    /// Cross-check the exact multi-resource computation against brute force over every
    /// sub-occupancy, for two-resource blockers. A 1-cpu request reads off the cpu allowance, and
    /// a 1-cpu + 1-gpu request the smaller of the two.
    #[test]
    fn test_multi_resource_gap_matches_brute_force() {
        for capacity in [(8u32, 2u32), (12, 4), (16, 4), (20, 4)] {
            for blocker in [(4u32, 1u32), (3, 1), (2, 2), (5, 1)] {
                for occupants in [
                    vec![],
                    vec![(2u32, 1u32)],
                    vec![(3, 0)],
                    vec![(1, 1), (2, 0)],
                    vec![(2, 1), (2, 1), (1, 0)],
                    vec![(3, 1), (1, 0), (2, 0)],
                ] {
                    let used = occupants
                        .iter()
                        .fold((0, 0), |(c, g), (oc, og)| (c + oc, g + og));
                    if used.0 > capacity.0 || used.1 > capacity.1 {
                        continue;
                    }
                    let expected = brute_force_gap(capacity, blocker, &occupants);

                    let mut rt = TestEnv::new();
                    let gpu = rt.new_named_resource("gpus");
                    let w =
                        rt.new_worker(&WorkerBuilder::new(capacity.0).res_sum("gpus", capacity.1));
                    let occ: Vec<_> = occupants
                        .iter()
                        .map(|(c, g)| {
                            let tb = TaskBuilder::new().cpus(*c);
                            rt.new_task(&if *g > 0 { tb.add_resource(gpu, *g) } else { tb })
                        })
                        .collect();
                    let r_h = rt.new_task(
                        &TaskBuilder::new()
                            .cpus(blocker.0)
                            .add_resource(gpu, blocker.1),
                    );
                    let cpu_only = rt.new_task_cpus(1);
                    let cpu_and_gpu = rt.new_task(&TaskBuilder::new().cpus(1).add_resource(gpu, 1));
                    let context = format!("C={capacity:?} r_h={blocker:?} occupants={occupants:?}");
                    assert_eq!(
                        compute_gap_occupied(&mut rt, r_h, cpu_only, w, &occ),
                        expected.0,
                        "cpu allowance, {context}"
                    );
                    assert_eq!(
                        compute_gap_occupied(&mut rt, r_h, cpu_and_gpu, w, &occ),
                        expected.0.min(expected.1),
                        "cpu+gpu allowance, {context}"
                    );
                }
            }
        }
    }

    /// Where the conservative bound is loose. A 4-cpu + 1-gpu blocker on 20 cpus and 4 gpus can
    /// never use more than 16 cpus, and while a 2-cpu + 1-gpu occupant runs it gets only 3 gpus,
    /// hence 12 cpus. So 4 cpus are never usable by it, whatever finishes first. The bound
    /// `C - M(C) - o = 20 - 16 - 2` charges the occupant's cpus as if they stayed forever and
    /// gives only 2.
    #[test]
    fn test_multi_resource_gap_is_larger_than_the_bound() {
        let mut rt = TestEnv::new();
        let gpu = rt.new_named_resource("gpus");
        let w = rt.new_worker(&WorkerBuilder::new(20).res_sum("gpus", 4));
        let occupant = rt.new_task(&TaskBuilder::new().cpus(2).add_resource(gpu, 1));
        let r_h = rt.new_task(&TaskBuilder::new().cpus(4).add_resource(gpu, 1));
        let r_l = rt.new_task_cpus(1);
        assert_eq!(compute_gap_occupied(&mut rt, r_h, r_l, w, &[occupant]), 4);
    }

    /// Too many distinct sub-occupancies for a multi-resource blocker: the conservative bound is
    /// used. Four 1-cpu + 1-gpu occupants and twelve cpu-only occupants of distinct sizes give
    /// 5 * 2^12 sub-occupancies. The exact gap would be 200 - 78 - 16 = 106; the bound is
    /// 200 - 16 - 82 = 102.
    #[test]
    fn test_multi_resource_gap_falls_back_to_bound_above_limit() {
        let mut rt = TestEnv::new();
        let gpu = rt.new_named_resource("gpus");
        let w = rt.new_worker(&WorkerBuilder::new(200).res_sum("gpus", 4));
        let mut occupants: Vec<_> = (0..4)
            .map(|_| rt.new_task(&TaskBuilder::new().cpus(1).add_resource(gpu, 1)))
            .collect();
        occupants.extend((1..=12).map(|c| rt.new_task_cpus(c)));
        let r_h = rt.new_task(&TaskBuilder::new().cpus(4).add_resource(gpu, 1));
        let r_l = rt.new_task_cpus(1);
        assert_eq!(compute_gap_occupied(&mut rt, r_h, r_l, w, &occupants), 102);
    }

    /// Multi-variant gaps are stored per resource id. A worker that lacks a resource with a lower
    /// id than one it has (here no "mem", id 1, but "gpus", id 2) must still get each value at the
    /// right id.
    ///
    /// Blocker variants: 8 cpus, or 2 cpus + 2 gpus. On 8 cpus + 3 gpus any mix uses at most
    /// 8 cpus and 2 gpus, so the gap is 0 cpus and 1 gpu, and nothing for the missing "mem".
    #[test]
    fn test_multi_variant_gap_keeps_resource_ids_when_worker_lacks_a_resource() {
        let mut rt = TestEnv::new();
        let mem = rt.new_named_resource("mem");
        let gpus = rt.new_named_resource("gpus");
        let w = rt.new_worker(&WorkerBuilder::new(8).res_sum("gpus", 3));
        let r_h = rt.new_task(
            &TaskBuilder::new()
                .cpus(8)
                .next_variant()
                .cpus(2)
                .add_resource(gpus, 2),
        );
        let CoreSplitMut {
            task_map,
            worker_map,
            scheduler_state,
            request_map,
            ..
        } = rt.core().split_mut();
        let h_rq = task_map.get_task(r_h).resource_rq_id;
        let res = &worker_map.get_worker(w).resources;
        let gap = scheduler_state
            .gap_cache
            .gap_resources(h_rq, res, iter::empty(), request_map)
            .unwrap();
        assert_eq!(gap.get(CPU_RESOURCE_ID), ResourceAmount::ZERO, "cpus");
        assert_eq!(gap.get(mem), ResourceAmount::ZERO, "mem");
        assert_eq!(gap.get(gpus), ResourceAmount::new_units(1), "gpus");
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
