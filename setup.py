import pathlib

from setuptools import find_packages, setup

here = pathlib.Path(__file__).parent.resolve()
long_description = (here / "README.md").read_text(encoding="utf-8")

setup(
    name="revo",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    version="0.0.1",
    description="RevoData assignment package.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="gvalentini85",
)
