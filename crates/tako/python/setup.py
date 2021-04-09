import os

from setuptools import find_packages, setup

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


def read(fname):
    return open(os.path.join(ROOT_DIR, fname)).read()


with open("requirements.txt") as reqs:
    requirements = [line.strip() for line in reqs.readlines()]

setup(
    name="tako",
    version="0.0.1",
    author="RSDS team",
    description="RSDS Rust worker",
    license="MIT",
    packages=find_packages(),
    install_requires=requirements,
)
