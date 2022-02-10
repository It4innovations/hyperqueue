import click
import dateutil.parser as p
import pandas as pd
import plotly.express as px
from events_type import *
from hq_event_wrapper import HQEventCLIWrapper, Events, HQEventFileWrapper


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


def create_alloc_chart(eventy):
    ques = eventy.get_by_type('autoalloc-allocation-qued')
    ques_id = ques.get_ids()
    data = []
    for id in ques_id:
        qued = eventy.get_by_type('autoalloc-allocation-qued').get_by_id(id)[0]
        que_started = eventy.get_by_type('autoalloc-allocation-started').get_by_id(id)[0]
        que_finished = eventy.get_by_type('autoalloc-allocation-finished').get_by_id(id)[0]
        que = [dict(id=f"qeue {id}", start=p.parse(qued.time), stop=p.parse(que_started.time), type='allocation-qeued'),
               dict(id=f"qeue {id}", start=p.parse(que_started.time), stop=p.parse(que_finished.time),
                    type='allocation-running')]
        data.extend(que)

    data = pd.DataFrame(data)
    fig = create_chart(data, x_start="start", x_end="stop", y="id", color='type')
    fig.u
    fig.show()


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


@click.group("alloc")
def cli_alloc():
    pass


@click.group()
def cli():
    pass


cli_job.add_command(create_workers_chart_CLI)
cli_job.add_command(create_workers_chart_JSON)
# cli_job()

cli_alloc.add_command(create_alloc_chart_JSON)
cli_alloc.add_command(create_alloc_chart_CLI)

cli.add_command(cli_job)
cli.add_command(cli_alloc)
cli()


