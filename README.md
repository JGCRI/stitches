# stitches
Amalgamate existing climate data to create monthly climate variable fields.

[![DOI](https://zenodo.org/badge/317969428.svg)](https://zenodo.org/badge/latestdoi/317969428)
[![build](https://github.com/JGCRI/stitches/actions/workflows/workflow.yml/badge.svg)](https://github.com/JGCRI/stitches/actions/workflows/workflow.yml)
[![pre-commit](https://github.com/JGCRI/stitches/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/JGCRI/stitches/actions/workflows/pre-commit.yml)

## Getting Started Using `stitches`
Jupyter notebooks hosted on `stitches` use functionality that is contained within the accompanying Python package.

> **NOTE**
> Ensure you are using Python >= 3.9. Calling `python` may use a different instance.  Some users may need to use `python3` or the like instead.


### Installation
To install for use, run the following:
```bash
python -m pip install git+https://github.com/JGCRI/stitches.git
```

To install package data that has already been pre-processed run the following:
```python
import stitches

stitches.install_package_data()
```

For users who would like to generate the package data locally, run the following:

```python
import stitches

stitches.generate_pkg_data()
```
but note that this will take several hours to run.

### Tutorial Jupyter Notebooks
|          Notebook           |                                Description                                |
|:---------------------------:|:-------------------------------------------------------------------------:|
| `stitches-quickstart.ipynb` | Simple tutorial to demonstrate how `stitches` can be used as an emulator. |

### Contributing
`stitches`  users and developers must agree to our community guidelines outlines in our community guidelines outlines in our
[Contributor Guidelines](CONTRIBUTING.md) and [Code of Conduct](CODE_OF_CONDUCT.md).
Open an issue to ask for help or report an issue ([how to open a GitHub issue](https://docs.github.com/en/enterprise-server@3.1/issues/tracking-your-work-with-issues/creating-an-issue)).
