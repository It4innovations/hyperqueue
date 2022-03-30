import logging
from abc import ABC

from dataclasses import dataclass
from multiprocessing import Pool
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from tornado import ioloop, web
from glob import glob

import os
import humanize
import numpy as np
import pandas as pd
import tqdm
import pathlib
import json
from bokeh.io import save, show
from bokeh.models import (
    Column,
    Div,
    LayoutDOM,
    Panel,
    Tabs,
    ColumnDataSource,
    Button,
    CustomJS,
    Select,
    MultiChoice,
)
from bokeh.plotting import figure, curdoc
from bokeh.models.widgets import DataTable, DateFormatter, TableColumn
from bokeh.resources import CDN
from bokeh.layouts import column, row, widgetbox, gridplot
from jinja2 import Template

from . import report
from ..benchmark.database import Database, DatabaseRecord
from ..utils import ensure_directory
from .common import (
    ProcessStats,
    average,
    create_database_df,
    get_process_aggregated_stats,
    groupby_environment,
    groupby_workload,
    pd_print_all,
)
from .monitor import (
    generate_cluster_report,
    create_page,
    render_profiling_data,
    create_global_resources_df,
    render_global_resource_usage,
    render_nodes_resource_usage,
    create_per_process_resources_df,
    render_process_resource_usage,
)
from .report import ClusterReport


@dataclass
class BenchmarkEntry:
    record: DatabaseRecord
    report: ClusterReport
    monitoring_report: Optional[Path]
    process_stats: Dict[Any, ProcessStats]


EntryMap = Dict[str, BenchmarkEntry]


def render(template: str, **kwargs) -> str:
    return Template(template).render(
        **kwargs, format_bytes=lambda v: humanize.naturalsize(v, binary=True)
    )


def style(level=0) -> Dict[str, Any]:
    return dict(margin=(5, 5, 5, 5 + 20 * level), sizing_mode="stretch_both")


def render_benchmark(entry: BenchmarkEntry):
    node_utilization = {
        node.hostname: {
            "cpu": average([average(record.resources.cpu) for record in records]),
            "memory": average([record.resources.mem for record in records]),
        }
        for (node, records) in entry.report.monitoring.items()
    }

    tabs = []
    tabs.append(
        Panel(
            child=Div(
                text=render(
                    """
<b>Duration</b>: {{ "%.4f"|format(benchmark.record.duration) }} s
{% if benchmark.process_stats %}
    <h3>Process utilization</h3>
    <table>
        <thead><th>Hostname</th><th>Key</th><th>Avg. CPU</th><th>Max. RSS</th></thead>
        <tbody>
            {% for (k, v) in benchmark.process_stats.items() %}
                <tr>
                    <td>{{ k[0] }}</td><td>{{ k[1] }}</td><td>{{ "%.2f"|format(v.avg_cpu) }} %</td>
                    <td>{{ format_bytes(v.max_rss) }}</td>
                </tr>
            {% endfor %}
        </tbody>
    </table>
{% endif %}
{% if node_utilization %}
    <h3>Node utilization</h3>
    <table>
        <thead><th>Hostname</th><th>Avg. CPU</th><th>Avg. memory</th></thead>
        <tbody>
            {% for (hostname, data) in node_utilization.items() %}
                <tr>
                    <td>{{ hostname }}</td><td>{{ "%.2f"|format(data["cpu"]) }} %</td>
                    <td>{{ "%.2f"|format(data["memory"]) }} %</td>
                </tr>
            {% endfor %}
        </tbody>
    </table>
{% endif %}
{% if benchmark.monitoring_report %}
    <a href='{{ benchmark.monitoring_report }}'>Cluster report</a>
{% endif %}
<a href='http://localhost:5006/'> Comparator </a>
""",
                    benchmark=entry,
                    node_utilization=node_utilization,
                )
            ),
            title="Results",
        )
    )
    # tabs.append(Panel(child=create_page(entry.report), title="Cluster report"))
    return Tabs(tabs=tabs)


def render_comparison(
    entries: [BenchmarkEntry],
    data: pd.DataFrame,
):
    tabs = []
    is_hq = len(entries) == len(data)

    # HQ Comparison
    if is_hq:
        reports = [entry.report for entry in entries]
        widgets = {
            "Profiling data": [],
            "Global usage": [],
            "Node usage": [],
            "Process usage": [],
        }
        for report in reports:
            per_node_df = create_global_resources_df(report.monitoring)
            per_process_df = create_per_process_resources_df(report.monitoring)

            name = report.directory.name
            widgets["Profiling data"].append(
                Tabs(tabs=[Panel(child=render_profiling_data(report), title=name)])
            )

            if not per_node_df.empty:
                widgets["Global usage"].append(
                    Tabs(
                        tabs=[
                            Panel(
                                child=render_global_resource_usage(report, per_node_df),
                                title=name,
                            )
                        ]
                    )
                )

            if not per_node_df.empty:
                widgets["Node usage"].append(
                    Tabs(
                        tabs=[
                            Panel(
                                child=render_nodes_resource_usage(report, per_node_df),
                                title=name,
                            )
                        ]
                    )
                )

            if not per_process_df.empty:
                widgets["Process usage"].append(
                    Tabs(
                        tabs=[
                            Panel(
                                child=render_process_resource_usage(
                                    report, per_process_df
                                ),
                                title=name,
                            )
                        ]
                    )
                )

        for key in widgets:
            tabs.append(Panel(child=row(widgets[key]), title=key))
    else:
        tabs.append(
            Panel(
                child=Div(
                    text="Comparison is currently supported for HQ monitoring only"
                ),
                title="Error",
            )
        )

    return Tabs(tabs=tabs)


def render_durations(title: str, durations: List[float]):
    hist, edges = np.histogram(durations, density=True, bins=50)

    fig = figure(title=title, width=400, height=300)
    fig.quad(
        top=hist,
        bottom=0,
        left=edges[:-1],
        right=edges[1:],
        fill_color="red",
        line_color="white",
    )
    fig.y_range.start = 0
    fig.xaxis.axis_label = "Duration [s]"
    return fig


def render_environment(
    level: int,
    entry_map: EntryMap,
    environment: str,
    environment_params: str,
    data: pd.DataFrame,
):
    tabs = []

    durations = data["duration"]
    table = durations.describe().to_frame().transpose()
    Columns = [TableColumn(field=Ci, title=Ci) for Ci in table.columns]
    tabs.append(
        Panel(
            child=DataTable(columns=Columns, source=ColumnDataSource(table)),
            title="Aggregated durations",
        )
    )

    runs = []
    items = sorted(data.itertuples(index=False), key=lambda v: v.index)
    for item in items:
        entry = entry_map.get(item.key)
        if entry is not None:
            benchmark_content = render_benchmark(entry)
            runs.append(Panel(child=benchmark_content, title=str(item.index)))
    tabs.append(Panel(child=Tabs(tabs=runs), title="Individual runs"))

    return Panel(
        child=Tabs(tabs=tabs),
        title=environment + f"({environment_params})",
    )


def render_workload(
    level: int,
    entry_map: EntryMap,
    workload: str,
    workload_params: str,
    data: pd.DataFrame,
):
    tabs = []
    for (group, group_data) in groupby_environment(data):
        tabs.append(
            render_environment(level + 1, entry_map, group[0], group[1], group_data)
        )
    return Panel(child=Tabs(tabs=tabs), title=workload + f"({workload_params})")


def generate_entry(args: Tuple[DatabaseRecord, Path]) -> Optional[BenchmarkEntry]:
    record, directory = args
    workdir = Path(record.benchmark_metadata["workdir"])
    key = record.benchmark_metadata["key"]
    try:
        report = ClusterReport.load(workdir)
    except FileNotFoundError:
        return None

    target_report_filename = directory / "monitoring" / (key + ".html")
    if report.monitoring:
        if not target_report_filename.is_file():
            generate_cluster_report(report, target_report_filename)
        target_report_filename = str(target_report_filename.relative_to(directory))
    else:
        target_report_filename = None

    return BenchmarkEntry(
        record=record,
        report=report,
        monitoring_report=target_report_filename,
        process_stats=get_process_aggregated_stats(report),
    )


def pregenerate_entries(database: Database, directory: Path) -> EntryMap:
    logging.info("Generating report files and statistics")

    entry_map = {}
    with Pool() as pool:
        args = [(record, directory) for record in database.records]
        for ((record, _), entry) in tqdm.tqdm(
            zip(args, pool.imap(generate_entry, args)), total=len(args)
        ):
            if entry is not None:
                entry_map[record.benchmark_metadata["key"]] = entry
    return entry_map


def generate_summary_html(database: Database, directory: Path) -> Path:
    page = create_summary_page(database, directory)
    ensure_directory(directory)
    result_path = directory / "index.html"
    save(page, result_path, title="Benchmarks", resources=CDN)
    return result_path


def summary_by_benchmark(df: pd.DataFrame, file):
    grouped = df.groupby(["workload", "workload-params", "env", "env-params"])[
        "duration"
    ]
    with pd_print_all():
        for (group, data) in sorted(grouped, key=lambda item: item[0]):
            result = data.describe().to_frame().transpose()
            print(" ".join(group), file=file)
            print(f"{result}\n", file=file)


def two_level_summary(
    df: pd.DataFrame,
    primary_grouping: Callable[[pd.DataFrame], Any],
    secondary_grouping: Callable[[pd.DataFrame], Any],
    file,
    print_total=False,
):
    primary_group = primary_grouping(df)

    with pd_print_all():
        for (group, data) in sorted(primary_group, key=lambda item: item[0]):
            print(" ".join(group), file=file)

            secondary_group = secondary_grouping(data)
            for (group, results) in sorted(secondary_group, key=lambda item: item[0]):
                stats = results["duration"].describe()
                duration_str = f"{stats['mean']:.4f} s"
                if stats["count"] > 1:
                    duration_str += f" (min={stats['min']:.4f}, max={stats['max']:.4f})"
                print(f"\t{' '.join(group)}: {duration_str}", file=file)
            if print_total:
                mean_duration = data["duration"].mean()
                print(f"(mean): {mean_duration:.4f} s", file=file)

            print(file=file)


def generate_comparison_html(
    benchmarks: List[str], database: Database, directory: Path
):
    entry_map = pregenerate_entries(database, directory)
    ensure_directory(directory.joinpath("comparisons"))

    df = create_database_df(database)
    data = df.loc[df["key"].isin(benchmarks)]

    benchmark_entries = []
    for benchmark in benchmarks:
        entry = entry_map.get(benchmark)
        if entry is not None:
            benchmark_entries.append(entry)

    comparison = render_comparison(benchmark_entries, data)
    save(
        comparison,
        directory.joinpath("comparisons", "_".join(benchmarks) + ".html"),
        title="Bench Comparison",
        resources=CDN,
    )


def generate_summary_text(database: Database, file):
    df = create_database_df(database)
    if df.empty:
        logging.warning("No data found")
        return

    def generate(f):
        print("Grouped by workload:", file=f)
        two_level_summary(df, groupby_workload, groupby_environment, f)

        print("Grouped by environment:", file=f)
        two_level_summary(
            df, groupby_environment, groupby_workload, f, print_total=True
        )

        print("Grouped by benchmark:", file=f)
        summary_by_benchmark(df, f)

    if isinstance(file, (str, Path)):
        ensure_directory(file.parent)
        with open(file, "w") as f:
            generate(f)
    else:
        generate(file)


def create_summary_page(database: Database, directory: Path):
    entry_map = pregenerate_entries(database, directory)
    df = create_database_df(database)

    tabs = []
    for (group, group_data) in groupby_workload(df):
        tabs.append(render_workload(0, entry_map, group[0], group[1], group_data))
    return Tabs(tabs=tabs)


def create_comparer_page(database: Database, directory: Path, addr: str):
    def create_href(addr, location, name):
        return Div(text=f"""<a href='{addr}/{str(location)}'>{str(name)}</a>""")

    entry_map = pregenerate_entries(database, directory)
    df = create_database_df(database)
    ensure_directory(directory.joinpath("comparisons"))

    comparisons = {}
    for comparison in glob(str(directory.joinpath("comparisons/*"))):
        name = os.path.basename(comparison)
        comparisons[name] = create_href(
            addr, Path("comparisons").joinpath(name), os.path.splitext(name)[0]
        )

    comparisons_col = column(list(comparisons.values()))

    def callback():
        name = "_".join(bench_choice.value) + ".html"
        if comparisons.get(name) is None:
            data = df.loc[df["key"].isin(bench_choice.value)]

            entries = []
            for bench in bench_choice.value:
                if entry_map.get(bench) is not None:
                    entries.append(entry_map[bench])

            page = render_comparison(entries, data)
            location = Path("comparisons").joinpath(name)
            save(page, directory.joinpath(location), CDN, "Comparison")

            comparisons[name] = create_href(addr, location, name)
            comparisons_col.update(children=list(comparisons.values()))

    bench_opts = df["key"].to_list()
    bench_choice = MultiChoice(options=bench_opts)

    btn = Button(label="Compare", button_type="success")
    btn.on_click(callback)
    return column(row(bench_choice, btn), comparisons_col)
