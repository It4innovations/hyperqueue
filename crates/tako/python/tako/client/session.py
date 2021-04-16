import asyncio
import logging
import weakref
from typing import List

from .exception import TaskFailed
from .subworker import SubworkerDefinition
from .task import Task
from ..common import conn
from ..common.conn import SocketWrapper

logger = logging.getLogger(__name__)


class Session:
    def __init__(self, connection: SocketWrapper):
        self.connection = connection
        self.task_id_counter = 0
        self.tasks = weakref.WeakValueDictionary()

    async def _send_receive(self, message, expected_response, wait_list=None):
        await self.connection.send_message(message)
        while wait_list is None or wait_list:
            msg = await self.connection.receive_message()
            if msg["op"] == expected_response:
                return msg
            elif msg["op"] == "TaskFailed":
                self._task_failed(msg, wait_list)
            elif msg["op"] == "TaskUpdate":
                self._task_update(msg, wait_list)
            elif msg["op"] == "NewWorker" or msg["op"] == "LostWorker":
                continue
            elif msg["op"] == "Error":
                raise Exception("Error from server: {}", msg["message"])
            else:
                raise Exception(
                    f"Unexpected message received {msg}, expecting {expected_response}"
                )

    def wait(self, task: Task):
        self.wait_all([task])

    def wait_all(self, tasks: List[Task]):
        for task in tasks:
            _check_task(task, False)
        self._wait_all(tasks)

    def _wait_all(self, tasks):
        for task in tasks:
            _check_error(task)

        tasks = [task for task in tasks if not task.finished]
        if not tasks:
            return

        # def callback(msg):
        #    if msg["state"] == "Finished" or msg["state"] == "Invalid":
        #        id_set.remove(msg["id"])
        #    return not id_set

        message = {"op": "ObserveTasks", "tasks": [t._id for t in tasks]}
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._send_receive(message, None, set(tasks)))

        for task in tasks:
            _check_error(task)

    def cancel(self, task: Task):
        return self.cancel_all([task])

    def cancel_all(self, tasks: List[Task]):
        for task in tasks:
            _check_task(task, False)

        message = {"op": "CancelTasks", "tasks": [task._id for task in tasks]}
        loop = asyncio.get_event_loop()
        response = loop.run_until_complete(
            self._send_receive(message, "CancelTasksResponse")
        )

        for task_id in response["cancelled_tasks"]:
            self._finish_task(task_id, "Task canceled", None)

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
            self.tasks[task_id] = task
        loop = asyncio.get_event_loop()
        response = loop.run_until_complete(
            self._send_receive(
                {"op": "NewTasks", "tasks": task_defs}, "NewTasksResponse"
            )
        )

    def register_subworker(self, sw_def: SubworkerDefinition):
        message = sw_def.as_dict()
        message["op"] = "RegisterSubworker"
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._send_receive(message, "Ok"))

    def overview(self):
        message = {"op": "GetOverview"}
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self._send_receive(message, "Overview"))

    def _task_failed(self, msg, wait_list):
        error = msg["info"]["message"]
        self._finish_task(msg["id"], error, wait_list)
        for task_id in msg["cancelled_tasks"]:
            self._finish_task(task_id, error, wait_list)

    def _task_update(self, msg, wait_list):
        assert msg["state"] == "Finished" or msg["state"] == "Invalid"
        self._finish_task(msg["id"], None, wait_list)

    def _finish_task(self, task_id, error=None, wait_list=None):
        task = self.tasks.get(task_id)
        if task is None:
            return
        task.finished = True
        task.error = error
        if wait_list and task in wait_list:
            wait_list.remove(task)


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


def _check_error(task):
    if task.error:
        raise TaskFailed(f"Task {task._id} failed\n{task.error}")
