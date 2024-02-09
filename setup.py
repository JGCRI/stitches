"""Setup script for the stitches package."""

import setuptools

#  You might fetch the version dynamically depending on your chosen method 
# with setups-scm; this provides an alternative example
def get_version():
    import os
    version_filepath = os.path.join(os.path.dirname(__file__), "stitches", "__init__.py")
    with open(version_filepath) as f:
        for line in f.readlines():
            if line.startswith("__version__"):
                return line.split("=")[-1].strip().strip('"')
    raise RuntimeError("Unable to find version string.")


def readme():
    """Read and return the content of the README.md file."""
    with open("README.md") as f:
        return f.read()


setuptools.setup(
    name="stitches-emulate",  # PyPI installation name
    version=get_version(),
    long_description=readme(),
    long_description_content_type="text/markdown",  # For reading your Markdown README
)
