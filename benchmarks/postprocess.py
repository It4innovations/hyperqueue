import logging
from pathlib import Path

import typer

from src.postprocessing.monitor import generate, serve
from src.postprocessing.report import ClusterReport

app = typer.Typer()


@app.command()
def cluster_serve(
        directory: Path = typer.Argument(..., exists=True, file_okay=False),
        port: int = 5555
):
    """Serve a HTML file with monitoring and profiling report for the given directory"""
    report = ClusterReport.load(directory)
    serve(report, port=port)


@app.command()
def cluster_generate(
        directory: Path = typer.Argument(..., exists=True, file_okay=False),
        output: Path = Path("out.html")
):
    """Generate a HTML file with monitoring and profiling report for the given directory"""
    report = ClusterReport.load(directory)
    generate(report, output=output)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(levelname)s:%(asctime)s:%(funcName)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    app()
