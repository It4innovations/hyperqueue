from pathlib import Path

from matplotlib import pyplot as plt


def render_chart_to_png(path: Path):
    plt.savefig(path)
