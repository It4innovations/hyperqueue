use crate::Priority;
use crate::internal::scheduler::state::PruneSchedule;
use crate::internal::server::core::{Core, CoreSplitMut};
use crate::internal::server::worker::Worker;
use crate::resources::ResourceRqId;
use std::cmp::Ordering;
use std::time::Instant;

// Defaults live in `SchedulerConfig::{prune_fixed_prefix, prune_global_max}` so the
// evaluation can sweep them; see `HQ_SCHED_PRUNE_F` / `HQ_SCHED_PRUNE_G`.

#[derive(Debug)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub(crate) struct PriorityCut {
    pub size: u32,
    pub blockers: Vec<(ResourceRqId, Option<u32>)>,
}

#[derive(Debug)]
pub(crate) struct TaskBatch {
    pub resource_rq_id: ResourceRqId,
    pub cuts: Vec<PriorityCut>,
    pub size: u32,
    pub limit: u32,
    pub limit_reached: bool,
    /// True if there is a cut in another batch that may be blocked by this batch
    pub is_blocker: bool,
    /// `cuts.len()` before `prune_progressive` truncated it. Recorded here rather
    /// than returned separately so the many `create_task_batches` callers keep
    /// their signature; `cuts.len()` is the after-count.
    pub cuts_before_prune: u32,
}

impl TaskBatch {
    pub fn new(resource_rq_id: ResourceRqId, limit: u32, limit_reached: bool) -> Self {
        TaskBatch {
            resource_rq_id,
            cuts: Vec::new(),
            size: 0,
            limit,
            limit_reached,
            is_blocker: false,
            cuts_before_prune: 0,
        }
    }
}

pub(crate) fn create_task_batches(
    core: &mut Core,
    now: Instant,
    custom_workers: Option<&[Worker]>,
) -> Vec<TaskBatch> {
    let CoreSplitMut {
        task_map: _,
        worker_map,
        task_queues,
        request_map,
        worker_groups,
        scheduler_state,
        ..
    } = core.split_mut();
    let prune_fixed_prefix = scheduler_state.config.prune_fixed_prefix;
    let prune_global_max = scheduler_state.config.prune_global_max;
    let prune_schedule = scheduler_state.config.prune_schedule;

    let queues: Vec<_> = task_queues.iter().filter(|q| !q.is_empty()).collect();
    if queues.is_empty() {
        return Vec::new();
    }

    let mut batches: Vec<_> = queues
        .iter()
        .map(|q| {
            let rqv = request_map.get(q.resource_rq_id);
            let limit = if rqv.is_multi_node() {
                let n_nodes = rqv.unwrap_first().n_nodes();
                let n_frees = worker_groups
                    .values()
                    .map(|g| {
                        g.worker_ids()
                            .map(|w_id| {
                                let worker = worker_map.get_worker(w_id);
                                if worker.is_free() { 1 } else { 0 }
                            })
                            .sum::<u32>()
                    })
                    .sum::<u32>();
                n_frees / n_nodes
            } else {
                custom_workers
                    .map(|ws| itertools::Either::Right(ws.iter()))
                    .unwrap_or(itertools::Either::Left(worker_map.get_workers()))
                    .filter(|w| w.is_capable_to_run_rqv(rqv, now))
                    .map(|w| {
                        let runnable = w
                            .sn_assignment()
                            .map(|a| a.free_resources.task_max_count(rqv))
                            .unwrap_or(0);
                        if runnable > 0 { runnable } else { 1 }
                    })
                    .sum::<u32>()
            };
            TaskBatch::new(q.resource_rq_id, limit, false)
        })
        .collect();

    let mut iters: Vec<_> = queues.iter().map(|q| q.iter_priority_sizes()).collect();
    let mut current: Vec<Option<_>> = iters.iter_mut().map(|it| it.next()).collect();
    let mut unique = None;
    let mut found = Vec::new();
    // The sweep step at which each batch last consumed tasks. A batch needs a new cut only if
    // some other batch consumed since then: otherwise the tasks of other requests above it are
    // the same as at its previous cut, so the cut would only repeat that one with a bigger size.
    let mut step = 0u32;
    let mut last_step: Vec<Option<u32>> = vec![None; batches.len()];

    loop {
        step += 1;
        found.clear();
        let mut highest_p = Priority::new(0);
        for (idx, c) in current.iter().enumerate() {
            if let Some((priority, _size)) = c {
                match highest_p.cmp(priority) {
                    Ordering::Equal => {
                        found.push(idx);
                    }
                    Ordering::Less => {
                        highest_p = *priority;
                        found.clear();
                        found.push(idx);
                    }
                    Ordering::Greater => { /* Do nothing */ }
                }
            }
        }
        if found.len() == 1 && unique == Some(found[0]) {
            let idx = found[0];
            let size = current[idx].unwrap().1;
            if unique == Some(idx) {
                last_step[idx] = Some(step);
                batches[idx].size += size;
                if batches[idx].size > batches[idx].limit {
                    batches[idx].size = batches[idx].limit;
                    batches[idx].limit_reached = true;
                    current[idx] = None;
                } else {
                    current[idx] = iters[idx].next();
                }
            }
        } else if found.is_empty() {
            break;
        } else {
            for idx in &found {
                let since = last_step[*idx];
                let changed = last_step
                    .iter()
                    .enumerate()
                    .any(|(i, s)| i != *idx && s.is_some_and(|s| since.is_none_or(|l| s >= l)));
                if !changed {
                    continue;
                }
                let size = batches[*idx].size;
                let higher_priorities: Vec<_> = batches
                    .iter_mut()
                    .enumerate()
                    .filter(|(i, b)| i != idx && (b.size > 0 || b.limit_reached))
                    .map(|(_, b)| {
                        b.is_blocker = true;
                        (b.resource_rq_id, (!b.limit_reached).then_some(b.size))
                    })
                    .collect();
                if !higher_priorities.is_empty() {
                    let cut = PriorityCut {
                        size,
                        blockers: higher_priorities,
                    };
                    batches[*idx].cuts.push(cut);
                }
            }
            for idx in &found {
                last_step[*idx] = Some(step);
                batches[*idx].size += current[*idx].unwrap().1;
                if batches[*idx].size > batches[*idx].limit {
                    batches[*idx].size = batches[*idx].limit;
                    batches[*idx].limit_reached = true;
                    current[*idx] = None;
                } else {
                    current[*idx] = iters[*idx].next();
                }
            }
            unique = if found.len() == 1 {
                Some(found[0])
            } else {
                None
            };
        }
    }
    batches.retain_mut(|b| {
        b.cuts_before_prune = b.cuts.len() as u32;
        b.size > 0
    });
    apply_global_cut_budget(
        &mut batches,
        prune_fixed_prefix,
        prune_global_max,
        prune_schedule,
    );
    batches
}

/// Cap the total number of priority conditions across all batches (G). See
/// `SchedulerConfig::prune_global_max` for why the total is the quantity that matters.
///
/// Every batch is seeded with the fixed prefix *unconditionally*, so the prefix is guaranteed even
/// when it overshoots the budget; whatever is left goes out round-robin.
fn apply_global_cut_budget(
    batches: &mut [TaskBatch],
    prefix: usize,
    mut budget: usize,
    schedule: PruneSchedule,
) {
    let total: usize = batches.iter().map(|b| b.cuts.len()).sum();
    if total <= budget {
        return;
    }
    let mut granted = vec![0; batches.len()];
    for (batch, g) in batches.iter().zip(granted.iter_mut()) {
        let take = batch.cuts.len().min(prefix);
        budget = budget.saturating_sub(take);
        *g += take;
    }

    while budget > 0 {
        for (batch, g) in batches.iter().zip(granted.iter_mut()) {
            if *g < batch.cuts.len() {
                *g += 1;
                budget -= 1;
                if budget == 0 {
                    break;
                }
            }
        }
    }
    for (batch, granted) in batches.iter_mut().zip(granted.iter()) {
        if *granted < batch.cuts.len() {
            prune_progressive(&mut batch.cuts, prefix, *granted, schedule);
        }
    }
}

fn prune_progressive<T>(
    vec: &mut Vec<T>,
    prefix_size: usize,
    size_limit: usize,
    schedule: PruneSchedule,
) {
    let original_len = vec.len();

    if original_len <= size_limit {
        return;
    }

    // The prefix is the whole budget (or more): keep the head and drop the rest. Also guards the
    // `remaining_slots - 1` division in `sample_indices`, which every sampled shape needs.
    if size_limit <= prefix_size + 1 {
        vec.truncate(size_limit);
        return;
    }

    let indices = sample_indices(prefix_size, size_limit, original_len, schedule);

    // To prune in-place
    for (i, target_idx) in indices.iter().enumerate().take(size_limit) {
        vec.swap(i, *target_idx);
    }

    vec.truncate(size_limit);
}

/// Which `size_limit` of `original_len` conditions survive, as a strictly increasing index list.
///
/// The invariants live here rather than in each shape: indices `0..prefix_size` always come first
/// (the guaranteed prefix `paper.tex` §6.4's exactness argument rests on), and the tail is forced
/// strictly increasing so no shape can emit a duplicate. Only the *spacing* of the tail differs
/// between shapes -- the length is always exactly `size_limit`, which is what makes the shapes
/// comparable at a fixed budget.
///
/// Callers must guarantee `prefix_size + 1 < size_limit < original_len`.
fn sample_indices(
    prefix_size: usize,
    size_limit: usize,
    original_len: usize,
    schedule: PruneSchedule,
) -> Vec<usize> {
    debug_assert!(prefix_size + 1 < size_limit && size_limit < original_len);

    let remaining_slots = size_limit - prefix_size;
    let source_pool_size = original_len - prefix_size;
    // Normalized 0.0 to 1.0 over the slots to fill.
    let step = |i: usize| i as f64 / (remaining_slots - 1) as f64;
    let last_offset = (source_pool_size - 1) as f64;

    // Offsets into the pool *after* the prefix, i.e. 0 maps to `prefix_size`. Every shape spans
    // `0..=source_pool_size - 1`; they differ only in how they distribute the slots across it.
    let offsets: Vec<usize> = match schedule {
        // Plain truncation: no sampling, just the next `remaining_slots` conditions.
        PruneSchedule::Head => (0..remaining_slots).collect(),
        PruneSchedule::Linear => (0..remaining_slots)
            .map(|i| (step(i) * last_offset).round() as usize)
            .collect(),
        PruneSchedule::Quadratic => (0..remaining_slots)
            .map(|i| {
                let t = step(i);
                (t * t * last_offset).round() as usize
            })
            .collect(),
        // Geometric spacing: `pool^t - 1` runs 0 -> pool - 1 over the same [0, 1], but hugs the
        // head far harder than the quadratic schedule does.
        PruneSchedule::Exponential => (0..remaining_slots)
            .map(|i| ((source_pool_size as f64).powf(step(i)) - 1.0).round() as usize)
            .collect(),
        // Score the whole pool and keep the lowest scores. Deterministic (a hash of the offset,
        // not an RNG), which the scheduler requires -- see `PruneSchedule::Random`.
        PruneSchedule::Random => {
            let mut scored: Vec<(u64, usize)> = (0..source_pool_size)
                .map(|offset| (splitmix64(offset as u64), offset))
                .collect();
            scored.sort_unstable();
            let mut picked: Vec<usize> = scored
                .into_iter()
                .take(remaining_slots)
                .map(|(_, offset)| offset)
                .collect();
            picked.sort_unstable();
            picked
        }
    };

    let mut indices: Vec<usize> = (0..prefix_size).collect();
    indices.reserve(remaining_slots);
    // Force the tail strictly increasing. `next_min` starts at `prefix_size`, so the prefix is
    // never re-sampled; and since `remaining_slots <= source_pool_size - 1`, bumping can never
    // push an index past `original_len - 1`.
    let mut next_min = prefix_size;
    for offset in offsets {
        let index = (prefix_size + offset).max(next_min);
        debug_assert!(index < original_len);
        indices.push(index);
        next_min = index + 1;
    }
    indices
}

/// Deterministic finalizer used only by [`PruneSchedule::Random`]. Chosen over a real RNG because
/// the scheduler must be reproducible: same state in, same batches out.
fn splitmix64(index: u64) -> u64 {
    let mut z = index.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

pub(crate) fn trim_to_unblocked(batches: &[TaskBatch]) -> Vec<TaskBatch> {
    batches
        .iter()
        .filter_map(|batch| {
            let size = batch.cuts.first().map_or(batch.size, |cut| cut.size);
            if size == 0 {
                return None;
            }
            Some(TaskBatch {
                resource_rq_id: batch.resource_rq_id,
                cuts: Vec::new(),
                size,
                limit: batch.limit,
                limit_reached: false,
                is_blocker: false,
                cuts_before_prune: 0,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prune_progressive() {
        let mut vec = (0..40).collect::<Vec<_>>();
        prune_progressive(&mut vec, 4, 100, PruneSchedule::Quadratic);
        assert_eq!(vec, (0..40).collect::<Vec<_>>());

        let mut vec = (0..1000).collect::<Vec<_>>();
        prune_progressive(&mut vec, 4, 32, PruneSchedule::Quadratic);
        assert_eq!(vec.len(), 32);
        assert_eq!(
            vec,
            vec![
                0, 1, 2, 3, 4, 5, 9, 16, 26, 38, 53, 71, 91, 115, 140, 169, 201, 235, 272, 311,
                353, 398, 446, 497, 550, 606, 665, 726, 790, 857, 927, 999
            ]
        );

        let mut vec = (0..40).collect::<Vec<_>>();
        prune_progressive(&mut vec, 4, 32, PruneSchedule::Quadratic);
        assert_eq!(vec.len(), 32);
        assert_eq!(
            vec,
            vec![
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                23, 24, 25, 27, 29, 32, 34, 36, 39
            ]
        );
    }

    /// A global budget (G) can hand a batch a limit at or just above the fixed prefix, which the
    /// quadratic sampler cannot express: one remaining slot makes its `i / (slots - 1)` step
    /// `0.0 / 0.0`. Those limits must keep the head and nothing else.
    #[test]
    fn test_prune_progressive_at_prefix_boundary() {
        for schedule in ALL_SCHEDULES {
            // At or just below the prefix there is nothing to sample: every shape truncates.
            for size_limit in 0..=5 {
                let mut vec = (0..40).collect::<Vec<_>>();
                prune_progressive(&mut vec, 4, size_limit, schedule);
                assert_eq!(
                    vec,
                    (0..size_limit as i32).collect::<Vec<_>>(),
                    "{schedule} did not truncate at size_limit {size_limit}"
                );
            }

            // One slot beyond the prefix still samples, and must not produce a duplicate: the
            // `i / (slots - 1)` step is `0.0 / 0.0` for a single remaining slot.
            let mut vec = (0..40).collect::<Vec<_>>();
            prune_progressive(&mut vec, 4, 6, schedule);
            assert_eq!(vec.len(), 6, "{schedule} lost a slot");
            assert_eq!(&vec[..4], &[0, 1, 2, 3], "{schedule} broke the prefix");
        }

        // The shipped shape reaches the far end of the tail with its one sampled slot.
        let mut vec = (0..40).collect::<Vec<_>>();
        prune_progressive(&mut vec, 4, 6, PruneSchedule::Quadratic);
        assert_eq!(vec, vec![0, 1, 2, 3, 4, 39]);
    }

    #[test]
    fn test_global_cut_budget_splits_across_batches() {
        fn batch(n_cuts: usize) -> TaskBatch {
            let mut b = TaskBatch::new(0.into(), 100, false);
            b.cuts = (0..n_cuts)
                .map(|i| PriorityCut {
                    size: i as u32,
                    blockers: Vec::new(),
                })
                .collect();
            b
        }
        let total = |bs: &[TaskBatch]| bs.iter().map(|b| b.cuts.len()).sum::<usize>();

        // Under budget: untouched.
        let mut batches = vec![batch(3), batch(3)];
        apply_global_cut_budget(&mut batches, 4, 32, PruneSchedule::Quadratic);
        assert_eq!(total(&batches), 6);

        let mut batches: Vec<_> = (0..8).map(|_| batch(3)).collect();
        apply_global_cut_budget(&mut batches, 4, 8, PruneSchedule::Quadratic);
        assert_eq!(total(&batches), 24);

        // Proportional: the bigger batch keeps more, and the budget is spent exactly.
        let mut batches = vec![batch(60), batch(10), batch(10)];
        apply_global_cut_budget(&mut batches, 4, 16, PruneSchedule::Quadratic);
        assert!(batches[0].cuts.len() > batches[1].cuts.len());
        assert_eq!(total(&batches), 16);

        let mut batches = vec![batch(60), batch(5), batch(5)];
        apply_global_cut_budget(&mut batches, 4, 40, PruneSchedule::Quadratic);
        assert_eq!(total(&batches), 40);

        let mut batches = vec![batch(30), batch(5), batch(5)];
        apply_global_cut_budget(&mut batches, 4, 39, PruneSchedule::Quadratic);
        assert_eq!(total(&batches), 39);
        assert_eq!(batches[0].cuts.len(), 29);
    }

    const ALL_SCHEDULES: [PruneSchedule; 5] = [
        PruneSchedule::Head,
        PruneSchedule::Linear,
        PruneSchedule::Quadratic,
        PruneSchedule::Exponential,
        PruneSchedule::Random,
    ];

    fn sample(schedule: PruneSchedule) -> Vec<usize> {
        sample_indices(4, 32, 1000, schedule)
    }

    /// The shapes are pinned by expected output, not by properties, so changing one shows up as a
    /// diff here. `quadratic` reproduces the vector `test_prune_progressive` asserted before the
    /// schedule was selectable -- the guard that this refactor changed nothing that ships.
    #[test]
    fn test_sample_indices_shapes() {
        assert_eq!(
            sample(PruneSchedule::Quadratic),
            vec![
                0, 1, 2, 3, 4, 5, 9, 16, 26, 38, 53, 71, 91, 115, 140, 169, 201, 235, 272, 311,
                353, 398, 446, 497, 550, 606, 665, 726, 790, 857, 927, 999
            ]
        );
        assert_eq!(
            sample(PruneSchedule::Head),
            (0..32).collect::<Vec<_>>(),
            "head must be plain truncation"
        );
        assert_eq!(
            sample(PruneSchedule::Linear),
            vec![
                0, 1, 2, 3, 4, 41, 78, 115, 151, 188, 225, 262, 299, 336, 373, 409, 446, 483, 520,
                557, 594, 630, 667, 704, 741, 778, 815, 852, 888, 925, 962, 999
            ]
        );
        assert_eq!(
            sample(PruneSchedule::Exponential),
            vec![
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 16, 20, 25, 31, 39, 49, 63, 80, 103,
                132, 169, 218, 280, 361, 466, 600, 774, 999
            ],
            "exponential must hug the head harder than quadratic"
        );
        assert_eq!(
            sample(PruneSchedule::Random),
            vec![
                0, 1, 2, 3, 52, 72, 124, 137, 177, 190, 200, 299, 324, 381, 449, 452, 476, 562,
                590, 660, 667, 673, 693, 714, 804, 853, 873, 892, 967, 979, 980, 996
            ]
        );
    }

    /// Whatever the shape, the index list has to be usable: exactly `size_limit` entries, the
    /// prefix intact, strictly increasing, and in range. Without this a shape could silently
    /// duplicate or drop conditions, which would void every quality comparison between shapes.
    #[test]
    fn test_sample_indices_invariants() {
        for schedule in ALL_SCHEDULES {
            for (prefix, size_limit, len) in [
                (4, 32, 1000),
                (4, 32, 40),
                (4, 6, 40),
                (0, 8, 100),
                (10, 12, 13),
            ] {
                let indices = sample_indices(prefix, size_limit, len, schedule);
                let what = format!("{schedule} at ({prefix}, {size_limit}, {len})");
                assert_eq!(indices.len(), size_limit, "{what}: wrong length");
                assert_eq!(
                    &indices[..prefix],
                    (0..prefix).collect::<Vec<_>>(),
                    "{what}: prefix not preserved"
                );
                assert!(
                    indices.windows(2).all(|w| w[0] < w[1]),
                    "{what}: not strictly increasing: {indices:?}"
                );
                assert!(
                    indices.iter().all(|&i| i < len),
                    "{what}: index out of range: {indices:?}"
                );
            }
        }
    }

    /// The point of the whole experiment: at a fixed budget the shapes are interchangeable in
    /// *cost* and differ only in *content*. If this ever fails, a shape is changing the size of
    /// the model and the comparison is no longer controlled.
    #[test]
    fn test_schedules_are_count_equivalent() {
        for schedule in ALL_SCHEDULES {
            let mut vec = (0..1000).collect::<Vec<_>>();
            prune_progressive(&mut vec, 4, 32, schedule);
            assert_eq!(vec.len(), 32, "{schedule} changed the surviving count");
        }
    }
}
