import logging
from pathlib import Path

import typer

from src.benchutils import load_database
from src.postprocessing.monitor import generate_cluster_report, serve_cluster_report
from src.postprocessing.overview import generate_summary_html, generate_summary_text
from src.postprocessing.report import ClusterReport

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


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s:%(asctime)s:%(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    app()
