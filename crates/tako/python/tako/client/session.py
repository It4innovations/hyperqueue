import logging
import asyncio

from .exception import TaskFailed
from .subworker import SubworkerDefinition
from .task import Task
from ..common import conn
from ..common.conn import SocketWrapper
from typing import List


logger = logging.getLogger(__name__)



class Session:

    def __init__(self, connection: SocketWrapper):
        self.connection = connection
        self.task_id_counter = 0

    async def _send_receive(self, message, expected_response, callback=None):
        await self.connection.send_message(message)
        while True:
            msg = await self.connection.receive_message()
            if msg["op"] == expected_response:
                if (callback and callback(msg)) or not callback:
                    return msg
                else:
                    continue
            if msg["op"] == "TaskFailed":
                self._task_failed(msg)
            if msg["op"] == "Error":
                raise Exception("Error from server: {}", msg["message"])
            raise Exception(f"Unexpected message received {msg}, expecting {expected_response}")

    def wait(self, task):
        self.wait_all([task])

    def wait_all(self, tasks):
        for task in tasks:
            _check_task(task, False)
        self._wait_all(tasks)

    def _wait_all(self, tasks):
        id_list = [task._id for task in tasks]
        id_set = set(id_list)

        def callback(msg):
            if msg["state"] == "Finished" or msg["state"] == "Invalid":
                id_set.remove(msg["id"])
            return not id_set

        message = {"op": "ObserveTasks", "tasks": id_list}
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._send_receive(message, "TaskUpdate", callback=callback))

    def _new_task_id(self):
        self.task_id_counter += 1
        return self.task_id_counter

    def submit(self, tasks: List[Task]):
        task_defs = []
        for task in tasks:
            if task._id is not None:
                raise Exception("Task was already submitted")
            assert task.deps is None
            task_id = self._new_task_id()
            task._id = task_id
            task_def = {
                "id": task_id,
                "body": task.body,
                "type_id": task.type_id,
            }
            if task.keep:
                task_def["keep"] = True
            task_defs.append(task_def)
        loop = asyncio.get_event_loop()
        response = loop.run_until_complete(self._send_receive({
           "op": "NewTasks",
           "tasks": task_defs
        }, "NewTasksResponse"))

    def register_subworker(self, sw_def: SubworkerDefinition):
        message = sw_def.as_dict()
        message["op"] = "RegisterSubworker"
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._send_receive(message, "Ok"))

    def overview(self):
        message = {"op": "GetOverview"}
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self._send_receive(message, "Overview"))

    @staticmethod
    def _task_failed(msg):
        task_id = msg["id"]
        message = msg["info"]["message"]
        raise TaskFailed(f"Task {task_id} failed\n{message}")


def connect(path):
    logger.debug("Connecting to tako server %s", path)
    loop = asyncio.get_event_loop()
    connection = loop.run_until_complete(conn.connect_to_unix_socket(path))
    return Session(connection)


def _check_task(task, keep_check):
    if not isinstance(task, Task):
        raise Exception(f"{task} is not instance of Task")
    if task._id is None:
        raise Exception(f"{task} is not submitted")
    if keep_check and not task.keep:
        raise Exception(f"{task} does not have 'keep' flag")