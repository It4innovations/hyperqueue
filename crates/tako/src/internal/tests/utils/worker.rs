use crate::internal::messages::worker::FromWorkerMessage;

pub struct TestWorkerComm {
    messages: Vec<FromWorkerMessage>,
    worker_is_empty_notifications: usize,
    start_task_notifications: usize,
}

impl TestWorkerComm {
    pub fn new() -> Self {
        TestWorkerComm {
            messages: Vec::new(),
            worker_is_empty_notifications: 0,
            start_task_notifications: 0,
        }
    }

    pub fn check_emptiness(&self) {
        assert!(self.messages.is_empty());
        assert_eq!(self.worker_is_empty_notifications, 0);
        assert_eq!(self.start_task_notifications, 0);
    }

    pub fn take_start_task_notifications(&mut self) -> usize {
        std::mem::take(&mut self.start_task_notifications)
    }

    pub fn check_start_task_notifications(&mut self, count: usize) {
        assert_eq!(self.take_start_task_notifications(), count);
    }

    pub fn send_message_to_server(&mut self, message: FromWorkerMessage) {
        self.messages.push(message);
    }

    pub fn notify_worker_is_empty(&mut self) {
        self.worker_is_empty_notifications += 1;
    }

    pub fn notify_start_task(&mut self) {
        self.start_task_notifications += 1;
    }
}
