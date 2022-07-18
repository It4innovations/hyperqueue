import datetime

import click
import dateutil.parser as p
import pandas as pd
import plotly.express as px
from events_type import *
from hq_event_wrapper import HQEventCLIWrapper, Events, HQEventFileWrapper
import numpy as np
import plotly.graph_objects as go


def events_to_workers_boxes(eventy, worker_ids):
    tasks_ids = eventy.get_by_type(TasksEvents).get_ids()
    workers_tasks = Events([])
    for i in tasks_ids:
        event = eventy.get_by_type(TasksEvents).get_by_id(i)
        workers_tasks.extend(event)

    workers_boxes = []
    for id in worker_ids:
        ev = workers_tasks.get_by('worker', id)
        for e in ev:
            worker_id = e.worker
            task_start_time = p.parse(e.time)
            task_finished_time = p.parse(workers_tasks.get_by_type('task-finished').get_by_id(e.id)[0].time) if len(
                workers_tasks.get_by_type('task-finished').get_by_id(e.id)) > 0 else None
            task = dict(Job_id=e.id, Start=task_start_time, Finish=task_finished_time,
                        Worker=worker_id, Job_str=f'Job {e.id}')
            workers_boxes.append(task)

    return pd.DataFrame(workers_boxes)


def create_workers_chart(eventy):
    worker_ids = eventy.get_by_type(WorkerEvents).get_ids()

    workers_boxes = events_to_workers_boxes(eventy, worker_ids)
    workers_boxes['Worker'] = workers_boxes['Worker'].astype(str)
    fig = create_chart(workers_boxes, x_start="Start", x_end="Finish", y="Job_id", color="Worker", text='Job_str',
                       facet_row='Worker')
    # fig = px.timeline(workers_boxes, x_start="Start", x_end="Finish", y="Job_id", color="Worker",
    #                   text='Job_str', facet_row='Worker')
    fig.update_yaxes(visible=False, matches=None)
    fig.show()


def create_chart(data, **kwargs):
    return px.timeline(data, **kwargs)


def worker_mean_cpu_usage(eventy, id):
    workers = eventy.get_by_type(WorkerEvents).get_by('auto_alloc_qeue', id)
    que_worker_cpu_usage = []
    for worker in workers:
        overviews = eventy.get_by_type('worker-overview').get_by_id(worker.id)
        cpus = []
        for overview in overviews:
            cpus.append(
                dict(cpus=overview.hw_state['worker_cpu_usage']['cpu_per_core_percent_usage'], time=overview.time))
        data_frame = pd.DataFrame(cpus)
        data_frame['average'] = data_frame['cpus'].apply(lambda x: sum(x) / len(x))
        worker_cpus = []
        for data in data_frame.iterrows():
            start = p.parse(data[1]['time'])
            stop = start + datetime.timedelta(seconds=2)
            que = dict(id=f"qeue {id}", start=start, stop=stop, cpu_usage=data[1]['average'])

            worker_cpus.append(que)

        que_worker_cpu_usage.extend(worker_cpus)
        # worker_average = sum(data_frame['average']) / len(data_frame['average'])
        # que_worker_cpu_usage.append(worker_average)

    return que_worker_cpu_usage


ColorScale = [[0, "#00FF0C"], [0.1, "#90EF46"], [0.2, "#DDEF48"], [0.3, "#FBD641"], [0.4, "#FEAE35"], [0.5, "#FF8B2C"],
              [0.6, "#FF6C24"], [0.7, "#FF4E1D"], [0.8, "#FF3419"], [0.9, "#FF1C16"], [1.0, "#FF0915"]]


def create_alloc_chart(eventy):
    ques = eventy.get_by_type('autoalloc-allocation-qued')
    ques_id = ques.get_ids()
    data = []

    worker_connected = eventy.get_by_type('worker-connected')
    for worker in worker_connected:
        worker.auto_alloc_qeue = worker.extra['auto_allo_que']

    for id in ques_id:
        qued = eventy.get_by_type('autoalloc-allocation-qued').get_by_id(id)[0]
        que_started = eventy.get_by_type('autoalloc-allocation-started').get_by_id(id)[0]
        que_finished = eventy.get_by_type('autoalloc-allocation-finished').get_by_id(id)[0]

        que_worker_cpu_usage = worker_mean_cpu_usage(eventy, id)
        que_worker_cpu_usage[len(que_worker_cpu_usage) - 1]['type'] = 'allocation-running'
        data.extend(que_worker_cpu_usage)
        que = [dict(id=f"qeue {id}", start=p.parse(qued.time), stop=p.parse(que_started.time), type='allocation-qeued',
                    cpu_usage=0)]
        # dict(id=f"qeue {id}", start=p.parse(que_started.time), stop=p.parse(que_finished.time),
        #      type='allocation-running', cpu_usage=0)]
        data.extend(que)

    data = pd.DataFrame(data)
    fig = create_chart(data, x_start="start", x_end="stop", y="id", color='cpu_usage', text='type',
                       range_color=(0, 100), color_continuous_scale=ColorScale)

    fig.show()


def create_worker_cpu_usage_chart(eventy: Events, worker_id):
    worker_ids = eventy.get_workers_ids()
    cpu_usage_per_worker = []

    for id in worker_ids:
        worker_overviews = eventy.get_by_type('worker-overview').get_by_id(id)
        cpu_usages = []

        for worker_overview in worker_overviews:
            cpu_usage = worker_overview.hw_state['worker_cpu_usage']['cpu_per_core_percent_usage']
            cpu_usages.append([id, *cpu_usage, worker_overview.time])

        cpu_usage_per_worker.append(pd.DataFrame(cpu_usages))

    header = ['id']
    cpus_header = [f"cpu_{i}" for i in range(len(cpu_usage_per_worker[0].columns) - 2)]
    header.extend(cpus_header)
    header.append('time')
    for i in cpu_usage_per_worker:
        i.columns = header
    data = cpu_usage_per_worker[worker_id - 1]

    fig = go.Figure()
    for cpu in cpus_header:
        fig.add_trace(go.Scatter(x=data['time'], y=data[cpu], mode='lines+markers', name=cpu))
    fig.show()


@click.command(name="FILE-chart")
@click.argument("worker-id", type=int)
@click.argument("data-file")
def create_worker_cpu_usage_chart_JSON(worker_id, data_file):
    wrapper = HQEventFileWrapper(data_file)
    eventy = wrapper.get_objects()
    create_worker_cpu_usage_chart(eventy, worker_id)


@click.command(name="CLI-chart")
@click.argument("worker-id", type=int)
@click.option("--bin_file", default='events.bin')
@click.option("--bin_pwd", default='../')
@click.option('--hq_bin_file', default='target/debug')
def create_worker_cpu_usage_chart_CLI(worker_id, bin_file, bin_pwd, hq_bin_file):
    event_wrapper = HQEventCLIWrapper(bin_file, bin_pwd, hq_bin_file)
    eventy = event_wrapper.get_objects()
    create_worker_cpu_usage_chart(eventy, worker_id)


@click.command(name="CLI-chart")
@click.option("--bin_file", default='events.bin')
@click.option("--bin_pwd", default='../')
@click.option('--hq_bin_file', default='target/debug')
def create_workers_chart_CLI(bin_file, bin_pwd, hq_bin_file):
    event_wrapper = HQEventCLIWrapper(bin_file, bin_pwd, hq_bin_file)
    eventy = event_wrapper.get_objects()
    create_workers_chart(eventy)


@click.command(name="FILE-chart")
@click.argument('json-file')
def create_workers_chart_JSON(json_file):
    event_wrapper = HQEventFileWrapper(json_file)
    eventy = event_wrapper.get_objects()
    create_workers_chart(eventy)


@click.command(name="FILE-chart")
@click.argument('json-file')
def create_alloc_chart_JSON(json_file):
    wrapper = HQEventFileWrapper(json_file)
    eventy = wrapper.get_objects()
    create_alloc_chart(eventy)


@click.command(name="CLI-chart")
@click.option("--bin_file", default='events.bin')
@click.option("--bin_pwd", default='../')
@click.option('--hq_bin_file', default='target/debug')
def create_alloc_chart_CLI(bin_file, bin_pwd, hq_bin_file):
    event_wrapper = HQEventCLIWrapper(bin_file, bin_pwd, hq_bin_file)
    eventy = event_wrapper.get_objects()
    create_alloc_chart(eventy)


@click.group("jobs")
def cli_job():
    pass


@click.group(name="worker")
def cli_workers():
    pass


@click.group("alloc")
def cli_alloc():
    pass


@click.group()
def cli():
    pass


cli_workers.add_command(create_worker_cpu_usage_chart_JSON)
cli_workers.add_command(create_workers_chart_CLI)

cli_job.add_command(create_workers_chart_CLI)
cli_job.add_command(create_workers_chart_JSON)
# cli_job()

cli_alloc.add_command(create_alloc_chart_JSON)
cli_alloc.add_command(create_alloc_chart_CLI)

cli.add_command(cli_job)
cli.add_command(cli_alloc)
cli.add_command(cli_workers)
cli()
