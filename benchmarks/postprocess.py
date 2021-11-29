import logging
from pathlib import Path

import typer

from src.benchmark.database import Database
from src.materialization import DEFAULT_DATA_JSON
from src.postprocessing.monitor import generate_cluster_report, serve_cluster_report
from src.postprocessing.report import ClusterReport
from src.postprocessing.summary import generate_summary

app = typer.Typer()


@app.command()
def cluster_serve(
        directory: Path = typer.Argument(..., exists=True, file_okay=False),
        port: int = 5555
):
    """Serve a HTML file with monitoring and profiling report for the given directory"""
    report = ClusterReport.load(directory)
    serve_cluster_report(report, port=port)


@app.command()
def cluster_generate(
        directory: Path = typer.Argument(..., exists=True, file_okay=False),
        output: Path = Path("out.html")
):
    """Generate a HTML file with monitoring and profiling report for the given directory"""
    report = ClusterReport.load(directory)
    generate_cluster_report(report, output=output)


@app.command()
def summary(
        path: Path = typer.Argument(..., exists=True, dir_okay=True),
        output: Path = Path("summary.txt")
):
    """Generate a simple text summary of benchmark results"""
    if path.is_file():
        database_path = path
    elif path.is_dir():
        database_path = path / DEFAULT_DATA_JSON
        assert database_path.exists()
    else:
        raise Exception(f"{path} is not a valid file or directory")

    database = Database(database_path)
    with open(output, "w") as f:
        generate_summary(database, f)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(levelname)s:%(asctime)s:%(funcName)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    app()
