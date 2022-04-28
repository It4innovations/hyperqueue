import os
import sys
from setuptools import setup, find_packages
from setuptools_rust import Binding, RustExtension

if sys.version_info.major < 3 or (
    sys.version_info.major == 3 and sys.version_info.minor < 6
):
    sys.exit("Python 3.6 or new is required")

VERSION = os.getenv("HQ_BUILD_VERSION") if not None else os.getenv("CARGO_PKG_VERSION")

with open("requirements.txt") as reqs:
    requirements = [line.strip() for line in reqs.readlines()]

setup(
    name="hyperqueue",
    descriptions="HyperQueue Python API",
    author="HyperQueue team",
    email="martin.surkovsky@vsb.cz",
    version=VERSION,
    rust_extensions=[
        RustExtension("pyhq", binding=Binding.PyO3, path="./pyhq/Cargo.toml")
    ],
    package_dir={"": "python"},
    install_requires=requirements,
    zip_safe=False,
)
