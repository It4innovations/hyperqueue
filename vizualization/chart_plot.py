import matplotlib
from matplotlib.dates import date2num, MinuteLocator, AutoDateFormatter, SecondLocator, MicrosecondLocator
import matplotlib.pyplot as plt
from matplotlib.collections import PolyCollection
from hq_event_wrapper import HQEventCLIWrapper, Events, HQEventFileWrapper
from events_type import *
import click
import dateutil.parser as p


def create_boxes(tasks):
    boxes = []

    for worker_task in tasks:
        for task in worker_task[0]:
            del (task[2])
            thickness = task[3]
            del (task[3])
            box = create_box(task, thickness)
            boxes.append(box)

    return boxes


def create_box(task_repre, thickness):
    return [(date2num(task_repre[0]), task_repre[2] - thickness),
            (date2num(task_repre[0]), task_repre[2] + thickness),
            (date2num(task_repre[1]), task_repre[2] + thickness),
            (date2num(task_repre[1]), task_repre[2] - thickness),
            (date2num(task_repre[0]), task_repre[2] - thickness)]


def plot_chart(verts, colors, labels, labels_pos):
    bars = PolyCollection(verts, facecolors=colors, edgecolors=colors)

    fig, ax = plt.subplots()
    ax.add_collection(bars)
    ax.autoscale()
    loc = MinuteLocator(interval=5)
    ax.xaxis.set_major_locator(loc)
    ax.xaxis.set_major_formatter(AutoDateFormatter(loc))

    ax.set_yticks(labels_pos)
    ax.set_yticklabels(labels)
    plt.show()


def create_positions(tasks_per_workers):
    i = 1
    thickness = .1
    for worker_task in tasks_per_workers:
        position = i
        count = 0
        for task in zip(worker_task[0], worker_task[1]):
            if task[1]:
                task[0].append(position + (2 * thickness * count))
            else:
                position = i
                count = 0
                task[0].append(i)
            task[0].append(thickness)
            count += 1

        i = (position + (3 * thickness * count))
    return tasks_per_workers


def has_overlap(workers_tasks):
    real_out = []
    for boxes in workers_tasks:
        times = []
        duration = []
        starts = []
        for box in boxes:
            start = date2num(box[0])
            stop = date2num(box[1])
            starts.append(start)
            times.append((start, stop))
            duration.append(stop - start)
        output = [False]
        for i in range(len(duration)):
            if (i + 1) < len(starts):
                if duration[i] < abs(starts[i + 1] - starts[i]):
                    output.append(False)
                else:
                    output.append(True)
        real_out.append(output)
    return list(zip(workers_tasks, real_out))


def worker_label_pos(tasks_per_worker):
    return [tasks[0][0][3] for tasks in tasks_per_worker]


def events_to_workers_boxes(eventy, worker_ids):
    tasks_ids = eventy.get_by_type(TasksEvents).get_ids()
    workers_tasks = Events([])
    for i in tasks_ids:
        event = eventy.get_by_type(TasksEvents).get_by_id(i)
        workers_tasks.extend(event)

    workers_boxes = []
    for id in worker_ids:
        ev = workers_tasks.get_by('worker', id)
        boxes = []
        for e in ev:
            worker_id = e.worker
            task_start_time = p.parse(e.time)
            task_finished_time = p.parse(workers_tasks.get_by_type('task-finished').get_by_id(e.id)[0].time) if len(
                workers_tasks.get_by_type('task-finished').get_by_id(e.id)) > 0 else None
            task = [task_start_time, task_finished_time, worker_id]
            boxes.append(task)
        workers_boxes.append(boxes)

    return workers_boxes


def create_chart(eventy):
    worker_ids = eventy.get_by_type(WorkerEvents).get_ids()

    workers_boxes = events_to_workers_boxes(eventy, worker_ids)
    overlap_per_worker = has_overlap(workers_boxes)
    positions = create_positions(overlap_per_worker)
    workers_label_pos = worker_label_pos(positions)
    verts = create_boxes(positions)
    labels = [f'worker_{i}' for i in worker_ids]
    colors = [f'C{i}' for i in range(1, len(worker_ids) + 1, 1)]
    plot_chart(verts, colors, labels, workers_label_pos)


@click.command(name="CLI-chart")
@click.option("--bin_file", default='events.bin')
@click.option("--bin_pwd", default='../')
@click.option('--hq_bin_file', default='target/debug')
def create_timeline_chart_CLI(bin_file, bin_pwd, hq_bin_file):
    event_wrapper = HQEventCLIWrapper(bin_file, bin_pwd, hq_bin_file)
    eventy = event_wrapper.get_objects()
    create_chart(eventy)


@click.command(name="FILE-chart")
@click.argument('json-file')
def create_timeline_chart_JSON(json_file):
    event_wrapper = HQEventFileWrapper(json_file)
    eventy = event_wrapper.get_objects()
    create_chart(eventy)


@click.group()
def cli():
    pass


cli.add_command(create_timeline_chart_CLI)
cli.add_command(create_timeline_chart_JSON)
cli()
