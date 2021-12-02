import base64
import dataclasses
import datetime
import logging
import os
import subprocess
from collections import defaultdict
from pathlib import Path
from typing import Callable, Iterator, List, Optional, Tuple, TypeVar

import humanize
import pandas as pd
from bokeh.embed import file_html
from bokeh.io import save
from bokeh.layouts import column, gridplot
from bokeh.model import Model
from bokeh.models import (
    Column,
    ColumnDataSource,
    DatetimeTickFormatter,
    Div,
    HoverTool,
    LayoutDOM,
    NumeralTickFormatter,
    Panel,
    PreText,
    Range1d,
    Tabs,
)
from bokeh.palettes import d3
from bokeh.plotting import Figure, figure
from bokeh.resources import CDN
from cluster.cluster import Cluster, Node, ProcessInfo
from pandas import Timestamp
from tornado import ioloop, web

from ..monitoring.record import MonitoringRecord, ProcessRecord
from ..utils import ensure_directory
from .common import average
from .report import ClusterReport, MonitoringData

DATETIME_KEY = "datetime"
HOSTNAME_KEY = "hostname"
CPU_KEY = "cpu"
MEM_KEY = "mem"
NETWORK_CONNECTIONS_KEY = "net-connections"
NETWORK_READ_KEY = "net-read"
NETWORK_WRITE_KEY = "net-write"
IO_READ_KEY = "io-read"
IO_WRITE_KEY = "io-write"

PID_KEY = "pid"
AVG_CPU_KEY = "cpu-avg"
RSS_KEY = "rss"


@dataclasses.dataclass
class TimeRange:
    start: Timestamp
    end: Timestamp


def resample(df: pd.DataFrame, time_index: pd.Series, period="1S"):
    """Resamples the dataframe with the given `period` while using `time` as an index."""
    df.index = time_index
    df = df.resample(period).first()
    return df.interpolate()


def is_valid_process(process: ProcessInfo) -> bool:
    return process.key != "monitor"


def get_processes_by_hostname(cluster: Cluster, hostname: str) -> Iterator[ProcessInfo]:
    for (_, process) in cluster.get_processes(hostname=hostname):
        if is_valid_process(process):
            yield process


def select_colors(items: List) -> List[str]:
    palette = d3["Category20"][20]
    colors = []
    for index in range(len(items)):
        colors.append(palette[index % len(palette)])
    return colors


def prepare_time_range_figure(
    range: TimeRange, width=720, height=250, **kwargs
) -> Figure:
    fig = figure(
        plot_width=width,
        plot_height=height,
        x_range=[range.start, range.end],
        x_axis_type="datetime",
        **kwargs,
    )
    fig.xaxis.formatter = DatetimeTickFormatter(
        seconds=["%H:%M:%S"],
        minsec=["%H:%M:%S"],
        minutes=["%H:%M:%S"],
        hourmin=["%H:%M"],
        hours=["%H:%M"],
        days=["%d.%m."],
    )
    return fig


def render_node_per_cpu_pct_utilization(figure: Figure, df: pd.DataFrame):
    time = df[DATETIME_KEY]
    cpu_series = df[CPU_KEY]

    cpu_count = len(cpu_series.iloc[0])
    cpus = [
        resample(cpu_series.apply(lambda res: res[i]), time) for i in range(cpu_count)
    ]
    cpu_mean = resample(cpu_series.apply(lambda res: average(res)), time)

    figure.yaxis[0].formatter = NumeralTickFormatter(format="0 %")
    figure.y_range = Range1d(0, 1)

    data = {}
    tooltips = []

    for (i, cpu) in enumerate(cpus):
        key = f"cpu_{i}"
        data[key] = cpu / 100.0
        data[f"{key}_x"] = cpu.index
        tooltips.append((f"CPU #{i}", f"@{key}"))

    data["cpu_mean"] = cpu_mean / 100.0
    data["cpu_mean_x"] = cpu_mean.index
    tooltips.append(("CPU avg.", "@cpu_mean"))

    figure.add_tools(HoverTool(tooltips=tooltips))

    data = ColumnDataSource(data)

    colors = select_colors(cpus)
    for (i, color) in enumerate(colors):
        figure.line(
            x=f"cpu_{i}_x",
            y=f"cpu_{i}",
            color=color,
            legend_label=f"CPU #{i}",
            source=data,
        )

    figure.line(
        x="cpu_mean_x",
        y="cpu_mean",
        color="red",
        legend_label="Average CPU",
        line_dash="dashed",
        line_width=5,
        source=data,
    )


def render_node_mem_pct_utilization(figure: Figure, df: pd.DataFrame):
    mem = resample(df[MEM_KEY], df[DATETIME_KEY])
    figure.yaxis[0].formatter = NumeralTickFormatter(format="0 %")
    figure.y_range = Range1d(0, 1)

    figure.add_tools(HoverTool(tooltips=[("Memory", "@mem")]))

    data = ColumnDataSource(dict(mem=mem / 100.0, x=mem.index))
    figure.line(x="x", y="mem", color="red", legend_label="Memory", source=data)


def render_node_network_connections(figure: Figure, df: pd.DataFrame):
    connections_series = df[NETWORK_CONNECTIONS_KEY]
    connections = resample(connections_series, df[DATETIME_KEY])
    figure.y_range = Range1d(0, connections_series.max() + 10)

    figure.add_tools(HoverTool(tooltips=[("Connection count", "@count")]))
    data = ColumnDataSource(dict(count=connections, x=connections.index))
    figure.line(x="x", y="count", legend_label="Network connections", source=data)


def render_bytes_sent_received(
    figure: Figure, df: pd.DataFrame, label: str, read_col: str, write_col: str
):
    def accumulate(column):
        values = df[column]
        values = values - values.min()
        return resample(values, df[DATETIME_KEY])

    read = accumulate(read_col)
    write = accumulate(write_col)

    data = ColumnDataSource(
        dict(rx=read, rx_kb=read / 1024, tx=write, tx_kb=write / 1024, x=read.index)
    )
    tooltips = [("RX", "@rx_kb KiB"), ("TX", "@tx_kb KiB")]

    figure.add_tools(HoverTool(tooltips=tooltips))
    figure.yaxis[0].formatter = NumeralTickFormatter(format="0.0b")
    figure.line(
        x="x", y="rx", color="blue", legend_label="{} RX".format(label), source=data
    )
    figure.line(
        x="x", y="tx", color="red", legend_label="{} TX".format(label), source=data
    )


def render_node_network_activity(figure: Figure, df: pd.DataFrame):
    render_bytes_sent_received(
        figure,
        df,
        label="Network",
        read_col=NETWORK_READ_KEY,
        write_col=NETWORK_WRITE_KEY,
    )


def render_node_io_activity(figure: Figure, df: pd.DataFrame):
    render_bytes_sent_received(
        figure, df, label="I/O", read_col=IO_READ_KEY, write_col=IO_WRITE_KEY
    )


def render_node_utilization(df: pd.DataFrame) -> LayoutDOM:
    range = get_time_range(df[DATETIME_KEY])

    def render(fn, title: str):
        figure = prepare_time_range_figure(range)
        fn(figure, df)
        figure.title = title
        return figure

    cpu = render(render_node_per_cpu_pct_utilization, "CPU")
    mem = render(render_node_mem_pct_utilization, "Memory")
    net_activity = render(render_node_network_activity, "Network activity")
    io_activity = render(render_node_io_activity, "I/O activity")
    net_connections = render(render_node_network_connections, "Network connections")

    layout = [[cpu, net_activity, net_connections], [mem, io_activity]]
    return gridplot(layout)


def get_node_description(report: ClusterReport, hostname: str) -> str:
    description = hostname
    processes = get_processes_by_hostname(report.cluster, hostname)
    keys = sorted(p.key for p in processes if p.key)
    if keys:
        node_info = ", ".join(keys)
        node_info = node_info if len(node_info) < 30 else f"{node_info[:30]}…"
        description += f" ({node_info})"
    return description


def render_nodes_resource_usage(
    report: ClusterReport, resources_df: pd.DataFrame
) -> LayoutDOM:
    items = sorted(resources_df.groupby(HOSTNAME_KEY), key=lambda item: item[0])
    rows = []
    for (hostname, node_data) in items:
        utilization = render_node_utilization(node_data)

        header_text = get_node_description(report, hostname)
        header = Div(
            text=header_text,
            render_as_text=True,
            style={"font-size": "20px", "font-weight": "bold"},
        )

        rows.append(Column(children=[header, utilization]))
    return column(rows)


def render_global_percent_resource_usage(
    figure: Figure,
    source: ColumnDataSource,
    report: ClusterReport,
    hostnames: List[str],
):
    node_count = len(hostnames)
    figure.y_range = Range1d(0, node_count + 1)
    figure.yaxis.axis_label = "Usage (%)"

    # Draw resource usage
    colors = select_colors(hostnames)
    figure.vbar_stack(
        hostnames,
        x="x_start",
        width=1000.0,
        color=colors,
        line_color="black",
        legend_label=[get_node_description(report, hostname) for hostname in hostnames],
        source=source,
    )

    # Draw ceiling
    figure.line(
        source.data["x"],
        [node_count] * len(source.data["x"]),
        color="red",
        line_width=2,
    )


def create_global_resource_datasource_and_tooltips(
    time_index: pd.Series, df: pd.DataFrame, key: str
) -> Tuple[ColumnDataSource, List[Tuple[str, str]]]:
    data = dict(x=time_index, x_start=time_index + datetime.timedelta(milliseconds=500))
    items = sorted(df.groupby(HOSTNAME_KEY), key=lambda item: item[0])
    tooltips = []

    for (hostname, node_data) in items:
        records = node_data[node_data[HOSTNAME_KEY] == hostname]
        records.index = records[DATETIME_KEY]

        records = records[key]
        records = records.resample("1S").mean().reindex(time_index).ffill()
        data[hostname] = records / 100.0

        label_key = f"label_{hostname}"
        tooltips.append((hostname, f"@{label_key}"))
        data[label_key] = [f"{r:.02f} %" for r in records]
    source = ColumnDataSource(data)
    return (source, tooltips)


def render_global_resource_usage(
    report: ClusterReport, resources_df: pd.DataFrame
) -> LayoutDOM:
    df = resources_df[[DATETIME_KEY, HOSTNAME_KEY, CPU_KEY, MEM_KEY]].copy()
    # Average CPUs per record
    df[CPU_KEY] = df[CPU_KEY].apply(lambda entry: average(entry))
    df.index = df[DATETIME_KEY]

    range = get_time_range(df[DATETIME_KEY])
    time_index = df[DATETIME_KEY].resample("1S").count().index

    hostnames = sorted(df[HOSTNAME_KEY].unique())

    def render(title: str, key: str) -> Column:
        (source, tooltips) = create_global_resource_datasource_and_tooltips(
            time_index, df, key
        )

        figure = prepare_time_range_figure(
            range, width=1000, height=500, tooltips=tooltips
        )
        render_global_percent_resource_usage(figure, source, report, hostnames)
        return Column(children=[Div(text=title), figure])

    cpu = render("CPU usage", CPU_KEY)
    mem = render("Memory usage", MEM_KEY)

    return column([cpu, mem])


def create_global_resources_df(monitoring: MonitoringData) -> pd.DataFrame:
    """
    Converts monitoring data to a dataframe that contains global resource utilization records
    for each node in the cluster.
    """
    data = defaultdict(list)
    for (node, records) in monitoring.items():
        for record in records:
            record: MonitoringRecord = record
            data[DATETIME_KEY].append(pd.to_datetime(record.timestamp, unit="s"))
            data[HOSTNAME_KEY].append(node.hostname)
            data[CPU_KEY].append(record.resources.cpu)
            data[MEM_KEY].append(record.resources.mem)
            data[NETWORK_CONNECTIONS_KEY].append(record.resources.connections)
            data[NETWORK_READ_KEY].append(record.resources.net_read)
            data[NETWORK_WRITE_KEY].append(record.resources.net_write)
            data[IO_READ_KEY].append(record.resources.disk_read)
            data[IO_WRITE_KEY].append(record.resources.disk_write)
    return pd.DataFrame(data)


def create_per_process_resources_df(monitoring: MonitoringData) -> pd.DataFrame:
    """
    Converts monitoring data to a dataframe that contains resource utilization records
    per each observed process.
    """
    data = defaultdict(list)
    for (node, records) in monitoring.items():
        for record in records:
            record: MonitoringRecord = record
            for (pid, process) in record.processes.items():
                process: ProcessRecord = process

                data[DATETIME_KEY].append(pd.to_datetime(record.timestamp, unit="s"))
                data[HOSTNAME_KEY].append(node.hostname)
                data[PID_KEY].append(int(pid))
                data[AVG_CPU_KEY].append(process.cpu)
                data[RSS_KEY].append(process.rss)
    return pd.DataFrame(data)


def get_time_range(series: pd.Series) -> TimeRange:
    min_datetime = series.min() - datetime.timedelta(seconds=1)
    max_datetime = series.max()
    return TimeRange(start=min_datetime, end=max_datetime)


def render_flamegraph(flamegraph: Path):
    with open(flamegraph, "rb") as f:
        data = f.read()
        base64_content = base64.b64encode(data).decode()
        content = f"""<object type="image/svg+xml" width="1600px"
 data="data:image/svg+xml;base64,{base64_content}"></object>"""
        return Div(text=content)


def render_flamegraphs(report: ClusterReport):
    tabs = []
    for (key, flamegraph_file) in report.flamegraphs.items():
        widget = render_flamegraph(flamegraph_file)
        tabs.append(Panel(child=widget, title=key))

    return Tabs(tabs=tabs)


def load_process_output(path: Path) -> str:
    size = os.path.getsize(path)
    if size < 2 * 1024 * 1024:
        with open(path) as f:
            data = f.read()
    else:
        lines = 500
        head = subprocess.check_output(["head", "-n", str(lines), str(path)]).decode()
        tail = subprocess.check_output(["tail", "-n", str(lines), str(path)]).decode()
        data = f"""{head.strip()}

(end of first {lines} lines)
############
(...SKIPPED)
############
(start of last {lines} lines)

{tail}"""

    return f"(path: {path}, size: {humanize.naturalsize(size, binary=True)})\n{data}"


T = TypeVar("T")


def create_tabs(items: List[T], render_fn: Callable[[T], Model]):
    """
    Creates a tab for each passed item.
    If `render_fn` returns `None`, the tab for the corresponding node will not be created.
    """
    tabs = []
    for item in items:
        widget = render_fn(item)
        if widget is not None:
            tabs.append(widget)
    return Tabs(tabs=tabs)


def create_node_tabs(report: ClusterReport, render_fn: Callable[[Node], Model]):
    """
    Creates a tab for each node in the `report`.
    """
    nodes = sorted(report.cluster.nodes.values(), key=lambda node: node.hostname)
    return create_tabs(nodes, render_fn)


def render_output(report: ClusterReport):
    def render_node_output(node: Node) -> Optional[Model]:
        processes = [p for p in node.processes if is_valid_process(p)]

        def render_process_output(process: ProcessInfo) -> Optional[Model]:
            items = [("out", process.process.stdout), ("err", process.process.stderr)]

            def render_item(item: Tuple[str, str]):
                name, path = item
                path = Path(path)
                if path.is_file():
                    content = load_process_output(path)
                    content = PreText(text=content)
                    return Panel(child=content, title=name)

            return Panel(child=create_tabs(items, render_item), title=process.key)

        return Panel(
            child=create_tabs(processes, render_process_output), title=node.hostname
        )

    return create_node_tabs(report, render_node_output)


def render_process_resource_usage(report: ClusterReport, per_process_df: pd.DataFrame):
    def render_node(node: Node) -> Optional[Model]:
        node_data = per_process_df[per_process_df[HOSTNAME_KEY] == node.hostname]
        pids = sorted(node_data[PID_KEY].unique())

        def render_process(pid: int) -> Optional[Model]:
            process_data = node_data[node_data[PID_KEY] == pid]
            max_rss = process_data[RSS_KEY].max()
            avg_cpu = process_data[AVG_CPU_KEY].mean()
            process = [
                p for p in report.cluster.nodes[node.hostname].processes if p.pid == pid
            ]
            if not process:
                logging.warning(f"Process {node.hostname}/{pid} not found in cluster")
            process = process[0]

            range = get_time_range(process_data[DATETIME_KEY])

            mib_divisor = 1024.0 * 1024
            mem = resample(
                process_data[RSS_KEY] / mib_divisor, process_data[DATETIME_KEY]
            )
            mem_figure = prepare_time_range_figure(range)
            mem_figure.yaxis[0].formatter = NumeralTickFormatter()
            mem_figure.yaxis.axis_label = "RSS (MiB)"
            mem_figure.y_range = Range1d(0, max_rss / mib_divisor + 100.0)
            mem_figure.add_tools(HoverTool(tooltips=[("RSS (MiB)", "@rss")]))

            data = ColumnDataSource(dict(rss=mem, x=mem.index))
            mem_figure.line(
                x="x", y="rss", color="red", legend_label="Memory", source=data
            )

            cpu = resample(process_data[AVG_CPU_KEY], process_data[DATETIME_KEY])
            cpu_figure = prepare_time_range_figure(range)
            cpu_figure.yaxis[0].formatter = NumeralTickFormatter(format="0 %")
            cpu_figure.yaxis.axis_label = "Avg. CPU usage (%)"
            cpu_figure.y_range = Range1d(0, 1)
            cpu_figure.add_tools(HoverTool(tooltips=[("Avg. CPU usage", "@cpu")]))

            data = ColumnDataSource(dict(cpu=cpu / 100.0, x=cpu.index))
            cpu_figure.line(
                x="x", y="cpu", color="blue", legend_label="CPU usage (%)", source=data
            )

            summary = PreText(
                text=f"""
PID: {pid}
Key: {process.key}
Max. RSS: {humanize.naturalsize(max_rss, binary=True)}
Avg. CPU: {avg_cpu:.02f} %
""".strip()
            )

            columns = [summary, mem_figure, cpu_figure]
            return Panel(child=Column(children=columns), title=process.key)

        return Panel(child=create_tabs(pids, render_process), title=node.hostname)

    return create_node_tabs(report, render_node)


def create_page(report: ClusterReport):
    structure = []

    per_node_df = create_global_resources_df(report.monitoring)

    if not per_node_df.empty:
        structure += [
            (
                "Global utilization",
                lambda r: render_global_resource_usage(r, per_node_df),
            ),
            ("Node utilization", lambda r: render_nodes_resource_usage(r, per_node_df)),
        ]

    per_process_df = create_per_process_resources_df(report.monitoring)
    if not per_process_df.empty:
        structure += [
            (
                "Process utilization",
                lambda r: render_process_resource_usage(r, per_process_df),
            )
        ]

    structure.append(("Output", render_output))

    if report.flamegraphs:
        structure.append(("Flamegraphs", render_flamegraphs))

    tabs = []
    for name, fn in structure:
        widget = fn(report)
        if widget is not None:
            tabs.append(Panel(child=widget, title=name))

    return Tabs(tabs=tabs)


def generate_cluster_report(report: ClusterReport, output: Path):
    page = create_page(report)

    ensure_directory(output.parent)
    logging.info(f"Generating monitoring report into {output}")
    save(page, output, title="Cluster monitor", resources=CDN)


def serve_cluster_report(report: ClusterReport, port: int):
    class Handler(web.RequestHandler):
        def get(self):
            page = create_page(report)
            self.write(file_html(page, CDN, "Cluster report"))

    app = web.Application(
        [
            (r"/", Handler),
        ]
    )
    app.listen(port)

    logging.info(f"Serving report at http://0.0.0.0:{port}")
    ioloop.IOLoop.current().start()
