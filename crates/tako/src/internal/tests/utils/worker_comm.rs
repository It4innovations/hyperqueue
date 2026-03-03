use crate::internal::messages::worker::FromWorkerMessage;

pub struct TestWorkerComm {
    messages: Vec<FromWorkerMessage>,
    worker_is_empty_notifications: usize,
    task_started: usize,
}

impl Default for TestWorkerComm {
    fn default() -> Self {
        Self::new()
    }
}

impl TestWorkerComm {
    pub fn new() -> Self {
        TestWorkerComm {
            messages: Vec::new(),
            worker_is_empty_notifications: 0,
            task_started: 0,
        }
    }

    pub fn check_emptiness(&self) {
        assert!(self.messages.is_empty());
        assert_eq!(self.worker_is_empty_notifications, 0);
        assert_eq!(self.task_started, 0);
    }

    pub fn take_task_started(&mut self) -> usize {
        std::mem::take(&mut self.task_started)
    }

    pub fn check_task_started(&mut self, count: usize) {
        assert_eq!(self.take_task_started(), count);
    }

    pub fn send_message_to_server(&mut self, message: FromWorkerMessage) {
        self.messages.push(message);
    }

    pub fn take_messages(&mut self, count: usize) -> Vec<FromWorkerMessage> {
        if count != 0 {
            assert_eq!(count, self.messages.len());
        }
        std::mem::take(&mut self.messages)
    }

    pub fn notify_worker_is_empty(&mut self) {
        self.worker_is_empty_notifications += 1;
    }

    pub fn start_task(&mut self) {
        self.task_started += 1;
    }
}
