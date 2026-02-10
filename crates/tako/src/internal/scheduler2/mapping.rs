use crate::internal::scheduler2::TaskQueue;
use crate::internal::scheduler2::solver::SchedulingSolution;
use crate::internal::server::comm::Comm;
use crate::internal::server::core::Core;
use crate::internal::server::task::{ComputeTasksBuilder, Task, TaskRuntimeState};
use crate::resources::ResourceRqId;
use crate::{Map, ResourceVariantId, Set, TaskId, WorkerId};
use std::cmp::Reverse;

#[derive(Debug, Default)]
pub struct WorkerTaskMapping {
    pub(crate) sn_tasks_to_workers: Map<WorkerId, Vec<(TaskId, ResourceVariantId)>>,
    pub(crate) sn_steals: Map<WorkerId, Vec<TaskId>>,
}

pub(crate) fn create_task_mapping(
    core: &mut Core,
    mut solution: SchedulingSolution,
    mut assigned_not_running: Set<TaskId>,
) -> WorkerTaskMapping {
    let (task_map, worker_map, task_queues, resource_map, _) = core.split_all_mut();

    let mut assigned_not_running_queues: Map<(ResourceRqId, ResourceVariantId), Vec<_>> =
        Map::new();
    for task_id in assigned_not_running {
        let task = task_map.get_task(task_id);
        let (_, v_id) = task.get_assignments().unwrap();
        assigned_not_running_queues
            .entry((task.resource_rq_id, v_id))
            .or_insert_with(|| Vec::new())
            .push((task.priority(), task_id));
    }
    for queue in assigned_not_running_queues.values_mut() {
        queue.sort_unstable();
    }

    let mut result = WorkerTaskMapping::default();
    let has_more = solution.sn_counts.len() > 1;
    let mut new_steals = Vec::new();
    let mut worker_steals: Map<WorkerId, Vec<TaskId>> = Map::new();
    for (resource_rq_id, v_id, mut counts) in solution.sn_counts {
        let sum = counts.iter().map(|(_, c)| c).sum::<u32>();
        let assigned = assigned_not_running_queues.get_mut(&(resource_rq_id, v_id));
        let mut tasks = task_queues[resource_rq_id.as_usize()].take_tasks(sum, assigned);
        assert!(!tasks.is_empty());

        let mut task_idx = 0;
        'outer: loop {
            for (w_id, c) in counts.iter_mut() {
                if *c > 0 {
                    *c -= 1;
                    let task_id = tasks[task_idx];
                    let task = task_map.get_task_mut(task_id);
                    let new_state = match &task.state {
                        TaskRuntimeState::Waiting(_) => {
                            worker_map.get_worker_mut(*w_id).insert_sn_task(task_id);
                            result
                                .sn_tasks_to_workers
                                .entry(*w_id)
                                .or_default()
                                .push((task_id, v_id));
                            TaskRuntimeState::Assigned(*w_id)
                        }
                        TaskRuntimeState::Assigned(current_w) => {
                            assert_ne!(current_w, w_id);
                            worker_map
                                .get_worker_mut(*current_w)
                                .remove_sn_task(task_id, None);
                            new_steals.push(task_id);
                            worker_steals.entry(*current_w).or_default().push(task_id);
                            TaskRuntimeState::Stealing {
                                source: current_w.clone(),
                                target: Some(*w_id),
                            }
                        }
                        TaskRuntimeState::Stealing { .. } => {
                            todo!()
                        }
                        TaskRuntimeState::Running { .. }
                        | TaskRuntimeState::RunningMultiNode(_)
                        | TaskRuntimeState::Finished => {
                            unreachable!()
                        }
                    };
                    task.state = new_state;
                    task_idx += 1;
                    if task_idx >= tasks.len() {
                        break 'outer;
                    }
                }
            }
        }
    }

    // Retract unused assignet tasks
    for (_, task_id) in assigned_not_running_queues.into_values().flatten() {
        let task = task_map.get_task_mut(task_id);
        match &task.state {
            TaskRuntimeState::Assigned(worker_id) => {
                worker_map
                    .get_worker_mut(*worker_id)
                    .remove_sn_task(task_id, None);
                new_steals.push(task_id);
                worker_steals.entry(*worker_id).or_default().push(task_id);
                task.state = TaskRuntimeState::Stealing {
                    source: *worker_id,
                    target: None,
                }
            }
            _ => unreachable!(),
        }
    }

    if has_more {
        // Tasks are sorted by priority within resource_rq_id, so when there is just one, we do not need additional sorting
        result
            .sn_tasks_to_workers
            .iter_mut()
            .for_each(|(_, tasks)| {
                tasks.sort_by_key(|(task, _)| Reverse(task_map.get_task(*task).priority()));
            });
    }
    result
}
// for batch in task_batches {
//     let resource_rq_id = batch.resource_rq_id;
//     let rqv = resource_map.get(batch.resource_rq_id);
//     for v_id in rqv.variant_ids() {
//         let mut counts: Vec<_> = workers
//             .iter()
//             .filter_map(|w| {
//                 placements
//                     .get(&(w.id, batch.resource_rq_id, v_id))
//                     .map(|(_, var_idx)| (w.id, values[*var_idx as usize].round() as u32))
//             })
//             .collect();
//         let sum = counts.iter().map(|(_, c)| c).sum::<u32>();
//         if sum == 0 {
//             continue;
//         }
//         let tasks = task_queues[resource_rq_id.as_usize()].take_tasks(sum);
//         let mut task_idx = 0;
//         'outer: loop {
//             for (w_id, c) in &mut counts {
//                 if *c > 0 {
//                     *c -= 1;
//                     result
//                         .task_to_workers
//                         .entry(*w_id)
//                         .or_default()
//                         .push((tasks[task_idx], v_id));
//                     task_idx += 1;
//                     if task_idx >= tasks.len() {
//                         break 'outer;
//                     }
//                 }
//             }
//         }
//     }
//     //workers.iter().map(|w| )
// }

impl WorkerTaskMapping {
    pub fn dump(&mut self) {
        println!("=======================");
        for (w, ts) in &self.sn_tasks_to_workers {
            print!("w{w}:");
            for (task_id, variant) in ts {
                if variant.is_first() {
                    print!(" {}", task_id)
                } else {
                    print!(" {}/{}", task_id, variant)
                }
            }
            println!();
        }
    }

    pub fn send_messages(&self, core: &mut Core, comm: &mut impl Comm) {
        for (worker_id, tasks) in &self.sn_tasks_to_workers {
            let mut task_msg_builder = ComputeTasksBuilder::default();
            for (task_id, variant) in tasks {
                if !variant.is_first() {
                    // TODO: Send variant to worker
                    todo!();
                }
                let task = core.get_task_mut(*task_id);
                if let Some(msg) = task_msg_builder.add_task(task, Vec::new()) {
                    comm.send_worker_message(*worker_id, &msg);
                }
            }
            if let Some(msg) = task_msg_builder.into_last_message() {
                comm.send_worker_message(*worker_id, &msg);
            }
        }
    }
}
