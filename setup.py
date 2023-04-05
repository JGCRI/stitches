import re
from setuptools import setup, find_packages


def readme():
    with open('README.md') as f:
        return f.read()


def get_requirements():
    with open('requirements.txt') as f:
        return f.read().split()

version = re.search(r"__version__ = ['\"]([^'\"]*)['\"]", open('stitches/__init__.py').read(), re.M).group(1)

setup(
    name='stitches',
    version=version,
    packages=find_packages(),
    url='https://github.com/JGCRI/stitches',
    license='BSD 2-Clause',
    author='Abigail Snyder; Kalyn Dorheim; Claudia Tebaldi',
    author_email='abigail.snyder@pnnl.gov; kalyn.dorheim@pnnl.gov; claudia.tebaldi@pnnl.gov',
    description='Amalgamate existing climate data to create monthly climate variable fields',
    long_description=readme(),
    python_requires='>=3.9.0',
    include_package_data=True,
    install_requires=[
        "matplotlib>=3.3.2",
        "xarray>=2022.9.0",
        "numpy>=1.23.3",
        "pandas>=1.5.0,<2",
        "intake>=0.6.6",
        "intake-esm>=2021.8.17",
        "nc_time_axis>=1.4.1",
        "scikit-learn>=1.1",
        "gcsfs>=2022.5.0",
        "fsspec>=2022.5.0",
        "tqdm>=4.64.1"
    ]
)
