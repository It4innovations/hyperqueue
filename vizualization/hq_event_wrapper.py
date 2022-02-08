import subprocess as sp
import os
import json


class Event:
    pass


class Events(list):
    def __init__(self, data):
        self.extend(data)

    def get_by_type(self, event_type) -> list:
        if type(event_type) not in [list, tuple]:
            event_type = [event_type]
        return Events([i for i in self if type(i).__name__ in event_type])

    def get_by_id(self, id):
        return Events([i for i in self if i.id == id])

    def get_by(self, keyword, value):
        output = []
        for i in self:
            if keyword in type(i).__dict__.keys():
                if type(i).__dict__[keyword] == value:
                    output.append(i)

        return Events(output)

    def get_workers_ids(self):
        return Events([i.id for i in self.get_by_type('worker-connected')])

    def get_ids(self):
        output = []
        for i in self:
            if i.id not in output:
                output.append(i.id)

        return output


class HQEventCLIWrapper:

    def __init__(self, hq_event_bin_path: str, cwd: str = "", hq_bin_path: str = ""):
        self.hq_bin_path = os.path.join(hq_bin_path, "hq")
        self.hq_event_bin = hq_event_bin_path
        self.cwd = cwd
        self._get_events()

    def _invoke_at(func):
        def wrapper(self, *args, **kwargs):
            cwd = os.getcwd()
            os.chdir(self.cwd)
            dec_func = func(self, *args, **kwargs)
            os.chdir(cwd)
            return dec_func

        return wrapper

    @_invoke_at
    def _get_events(self):
        output = sp.run([self.hq_bin_path, "event-log", "export", self.hq_event_bin], capture_output=True)
        assert len(output.stdout) > 0
        json_events = output.stdout
        self.json_events_raw = json_events.decode("utf-8").split("\n")

    def get_objects(self) -> Events:
        objects = []
        for row in self.json_events_raw:
            if row != '':
                event = json.loads(row)

                if event['event']['type'] != 'worker-overview':
                    event['event']['time'] = event['time']
                    class_name = event['event']['type']
                    event['event'].pop('type', None)
                    new_event = type(class_name, (object,), event['event'])
                    objects.append(new_event())

        return Events(objects)


class HQEventFileWrapper(HQEventCLIWrapper):

    def __init__(self, json_path: str):
        self.json_path = json_path
        self._get_events()

    def _get_events(self):
        with open(self.json_path, 'r') as fp:
            self.json_events_raw = [row for row in fp.readlines()]
