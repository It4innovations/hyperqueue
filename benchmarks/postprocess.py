import logging
from pathlib import Path
from typing import List

import typer

from src.postprocessing.serve import serve_cluster_report, serve_summary_html
from src.postprocessing.monitor import generate_cluster_report
from src.postprocessing.overview import (
    generate_summary_html,
    generate_summary_text,
    generate_comparison_html,
)
from src.postprocessing.report import ClusterReport
from src.utils.benchmark import load_database

app = typer.Typer()

cluster = typer.Typer()
app.add_typer(cluster, name="cluster", help="Cluster utilization")

summary = typer.Typer()
app.add_typer(summary, name="summary", help="Result summary")


@cluster.command("serve")
def cluster_serve(
    directory: Path = typer.Argument(..., exists=True, file_okay=False),
    port: int = 5555,
):
    """Serve a HTML file with monitoring and profiling report for the given directory"""
    report = ClusterReport.load(directory)
    serve_cluster_report(report, port=port)


@cluster.command("generate")
def cluster_generate(
    directory: Path = typer.Argument(..., exists=True, file_okay=False),
    output: Path = Path("out.html"),
):
    """Generate a HTML file with monitoring and profiling report for the given directory"""
    report = ClusterReport.load(directory)
    generate_cluster_report(report, output=output)


@summary.command("text")
def summary_text(
    database_path: Path = typer.Argument(..., exists=True),
    output: Path = Path("summary.txt"),
):
    """Generate a simple text summary of benchmark results"""
    database = load_database(database_path)
    generate_summary_text(database, output)


@summary.command("html")
def summary_html(
    database_path: Path = typer.Argument(..., exists=True),
    directory: Path = Path("summary"),
):
    """Generate a HTML summary of benchmark results into the given `directory`"""
    database = load_database(database_path)
    file = generate_summary_html(database, directory)
    logging.info(f"You can find the summary in {file}")


@summary.command("serve")
def serve_html(
    database_path: Path = typer.Argument(..., exists=True),
    directory: Path = Path("summary"),
    port: int = 5555,
):
    """Serves a HTML summary of benchmark results, includes comparisons"""
    database = load_database(database_path)
    serve_summary_html(database, directory, port)


@summary.command("compare")
def summary_compare(
    benchmarks: List[str],
    database_path: Path = typer.Argument(..., exists=True),
    directory: Path = Path("summary"),
):
    """Creates a HTML summary of benchmark comparison for the given directory"""
    database = load_database(database_path)
    generate_comparison_html(benchmarks, database, directory)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s:%(asctime)s:%(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    app()
