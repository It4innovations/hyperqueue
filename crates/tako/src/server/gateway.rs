use crate::TaskId;
use crate::server::task::ErrorInfo;

pub trait Gateway {
    //fn is_kept(&self, task_id: TaskId) -> bool;
    fn send_client_task_finished(&mut self, task_id: TaskId);
    fn send_client_task_removed(&mut self, task_id: TaskId);
    fn send_client_task_error(
        &mut self,
        task_id: TaskId,
        consumers_id: Vec<TaskId>,
        error_info: ErrorInfo,
    );
}
