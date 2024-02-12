"""
The setup.py file for the stitches package.

The stitches package provides tools for stitching together climate model output
into a single, coherent dataset.
"""


import re

from setuptools import find_packages, setup

from ._version import __version__


def requirements():
    """
    Read and parse the 'requirements.txt' file.

    Returns a list of requirements specified in the 'requirements.txt' file.

    :return: A list of package requirements.
    :rtype: list
    """
    with open("requirements.txt") as f:
        return f.read().split()


# version = re.search(
#     r"__version__ = ['\"]([^'\"]*)['\"]", open("stitches/_version.py").read(), re.M
# ).group(1)

setup(
    name="stitches-emulator",
    version=__version__,  # version,
    packages=find_packages(),
    url="https://github.com/JGCRI/stitches",
    license="BSD 2-Clause",
    author="Abigail Snyder, Kalyn Dorheim, Claudia Tebaldi",
    author_email="abigail.snyder@pnnl.gov, kalyn.dorheim@pnnl.gov, claudia.tebaldi@pnnl.gov",
    description="Amalgamate existing climate data to create monthly climate variable fields",
    python_requires=">=3.9.0",
    include_package_data=True,
    install_requires=requirements(),
    extras_require={
        "dev": [
            "pytest>=7.4.3",
            "build>=0.5.1",
            "nbsphinx>=0.8.6",
            "setuptools>=57.0.0",
            "sphinx>=7",
            "sphinx_book_theme>=1",
            "sphinx-click>=5.1",
            "sphinx_copybutton>=0.5",
            "twine~=3.4.1",
            "pre-commit>=3.6.0",
        ]
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: Scientific/Engineering :: Atmospheric Science",
    ],
)
