import re

from setuptools import find_packages, setup


def readme():
    with open("README.md") as f:
        return f.read()


def requirements():
    with open("requirements.txt") as f:
        return f.read().split()


version = re.search(
    r"__version__ = ['\"]([^'\"]*)['\"]", open("stitches/__init__.py").read(), re.M
).group(1)

setup(
    name="stitches",
    version=version,
    packages=find_packages(),
    url="https://github.com/JGCRI/stitches",
    license="BSD 2-Clause",
    author="Abigail Snyder; Kalyn Dorheim; Claudia Tebaldi",
    author_email="abigail.snyder@pnnl.gov; kalyn.dorheim@pnnl.gov; claudia.tebaldi@pnnl.gov",
    description="Amalgamate existing climate data to create monthly climate variable fields",
    long_description=readme(),
    python_requires=">=3.9.0",
    include_package_data=True,
    install_requires=requirements(),
)
