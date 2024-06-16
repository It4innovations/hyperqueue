from pathlib import Path

from matplotlib import pyplot as plt


def render_chart(path: Path):
    assert path.suffix == ""
    plt.savefig(f"{path}.png")
    plt.savefig(f"{path}.pdf")
