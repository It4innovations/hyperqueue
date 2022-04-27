import logging

from dataclasses import dataclass
from multiprocessing import Pool
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from glob import glob

import os
import humanize
import numpy as np
import pandas as pd
import tqdm
from bokeh.io import save
from bokeh.models import (
    Div,
    Panel,
    Tabs,
    Button,
    MultiChoice,
)
from bokeh.plotting import figure
from bokeh.resources import CDN
from bokeh.layouts import column, row
from jinja2 import Template

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
    with open(
        os.path.join(os.path.dirname(__file__), "templates/benchmark.html")
    ) as file:
        template = file.read()

    node_utilization = {
        node.hostname: {
            "cpu": average([average(record.resources.cpu) for record in records]),
            "memory": average([record.resources.mem for record in records]),
        }
        for (node, records) in entry.report.monitoring.items()
    }

    return render(template, benchmark=entry, node_utilization=node_utilization)


def render_hq_comparison(
    entries: [BenchmarkEntry],
    data: pd.DataFrame,
):
    def render_bench_subtab(child, name):
        return Tabs(tabs=[Panel(child=child, title=name)])

    tabs = []
    reports = [entry.report for entry in entries]
    widgets = {
        "Profiling data": [],
        "Global usage": [],
        "Node usage": [],
        "Process usage": [],
    }
    node_maxes = {}

    for key in widgets:
        for report in reports:
            name = report.directory.name
            panel = Tabs()

            if key == "Profiling data":
                panel = render_bench_subtab(render_profiling_data(report), name)
            elif key == "Global usage":
                per_node_df = create_global_resources_df(report.monitoring)
                panel = render_bench_subtab(
                    render_global_resource_usage(report, per_node_df), name
                )
            elif key == "Node usage":
                # Keep maximum of each subfigure for axis sync between rows
                per_node_df = create_global_resources_df(report.monitoring)
                child = render_nodes_resource_usage(report, per_node_df)
                figs = child.children[0].children[1].children
                for fig in figs:
                    figmax = fig.y_range.end
                    if node_maxes.get(fig.title.text) is not None:
                        node_maxes[fig.title.text] = max(
                            figmax, node_maxes[fig.title.text]
                        )
                    else:
                        node_maxes[fig.title.text] = figmax
                panel = render_bench_subtab(child, name)
            elif key == "Process usage":
                per_process_df = create_per_process_resources_df(report.monitoring)
                panel = render_bench_subtab(
                    render_process_resource_usage(report, per_process_df), name
                )
            widgets[key].append(panel)

    for w in widgets["Node usage"]:
        figs = w.tabs[0].child.children[0].children[1].children
        for fig in figs:
            ymax = node_maxes[fig.title.text]
            fig.y_range.end = ymax

    for key in widgets:
        tabs.append(Panel(child=row(widgets[key]), title=key))

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
    content = "<h3>Aggregated durations</h3>"
    with pd_print_all():
        table = data["duration"].describe().to_frame().transpose()
        table = table.to_html(justify="center")
        content += table

    items = sorted(data.itertuples(index=False), key=lambda v: v.index)
    for item in items:
        entry = entry_map.get(item.key)
        if entry is not None:
            content += render_benchmark(entry)

    return content


def render_workload(
    level: int,
    entry_map: EntryMap,
    workload: str,
    workload_params: str,
    data: pd.DataFrame,
):
    with open(
        os.path.join(os.path.dirname(__file__), "templates/workload.html")
    ) as file:
        template = file.read()

    environments = {}
    for (group, group_data) in groupby_environment(data):
        key = group[0] + "(" + group[1] + ") [" + workload + ":" + workload_params + "]"
        environments[key] = render_environment(
            level + 1, entry_map, group[0], group[1], group_data
        )
    return render(template, environments=environments)


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
    # to save the results
    with open(result_path, "w") as file:
        file.write(page)
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

    hq_benchmark_entries = []
    for benchmark in benchmarks:
        entry = entry_map.get(benchmark)
        if entry is not None:
            if entry.record.environment == "hq":
                hq_benchmark_entries.append(entry)
            else:
                print("Only hq comparison is currently supported")
                return

    comparison = render_hq_comparison(hq_benchmark_entries, data)
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
    with open(
        os.path.join(os.path.dirname(__file__), "templates/summary.html")
    ) as file:
        template = file.read()

    entry_map = pregenerate_entries(database, directory)
    df = create_database_df(database)

    workloads = {}
    for (group, group_data) in groupby_workload(df):
        name = group[0] + f"({group[1]})"
        workloads[name] = render_workload(0, entry_map, group[0], group[1], group_data)
    return render(template, workloads=workloads)


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

            page = render_hq_comparison(entries, data)
            location = Path("comparisons").joinpath(name)
            save(page, directory.joinpath(location), CDN, "Comparison")

            comparisons[name] = create_href(addr, location, name)
            comparisons_col.update(children=list(comparisons.values()))

    bench_opts = df["key"].to_list()
    bench_choice = MultiChoice(options=bench_opts)

    btn = Button(label="Compare", button_type="success")
    btn.on_click(callback)
    return column(row(bench_choice, btn), comparisons_col)
