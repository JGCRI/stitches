[![DOI](https://zenodo.org/badge/317969428.svg)](https://zenodo.org/badge/latestdoi/317969428)
[![build](https://github.com/JGCRI/stitches/actions/workflows/workflow.yml/badge.svg)](https://github.com/JGCRI/stitches/actions/workflows/workflow.yml)
[![pre-commit](https://github.com/JGCRI/stitches/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/JGCRI/stitches/actions/workflows/pre-commit.yml)
[![status](https://joss.theoj.org/papers/ad81e6a435c13ae644a7ca8cb0ffbc35/status.svg)](https://joss.theoj.org/papers/ad81e6a435c13ae644a7ca8cb0ffbc35)


# stitches
Amalgamate existing climate data to create monthly climate variable fields.

## Getting Started Using `stitches`
Jupyter notebooks hosted on `stitches` use functionality that is contained within the accompanying Python package.

> **NOTE**
> Ensure you are using Python >= 3.10. Calling `python` may use a different instance. Some users may need to use `python3` or the like instead.

`stitches` is tested on Python 3.10–3.13 across Linux, macOS, and Windows.

### Installation
To install for use, run the following:
```bash
pip install stitches-emulator
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
`stitches` users and developers must agree to the community guidelines set out in our
[Contributor Guidelines](CONTRIBUTING.md) and [Code of Conduct](CODE_OF_CONDUCT.md).
Open an issue to ask for help or report a problem ([how to open a GitHub issue](https://docs.github.com/en/enterprise-server@3.1/issues/tracking-your-work-with-issues/creating-an-issue)).

#### Cloning for development

This repository's history is large relative to its working tree, so a full clone
transfers far more than you need. Use a blobless clone, which fetches file
contents on demand:

```bash
git clone --filter=blob:none https://github.com/JGCRI/stitches.git
```

Then install the development extra and enable the hooks:

```bash
python -m pip install -e ".[dev]"
pre-commit install
```

#### Testing

```bash
pytest                       # offline suite, runs in seconds
pytest tests/regression      # assert scientific outputs are unchanged
pytest --network --slow --package-data   # full suite, downloads data
```

`stitches` produces scientific data, so output-changing modifications are held to
a higher bar: `tests/regression/` pins outputs against recorded golden artifacts,
and any intentional change must be justified in [`CHANGELOG.md`](CHANGELOG.md).
See [Contributor Guidelines](CONTRIBUTING.md) for the full workflow.

### Development plans

Ongoing modernization work is tracked in [`plans/`](plans/):

| Document | Purpose |
|---|---|
| [`development-plan.md`](plans/development-plan.md) | Current-state assessment, known defects, and prioritized workstreams |
| [`benchmarks-and-regression-testing.md`](plans/benchmarks-and-regression-testing.md) | Output-invariance harness, benchmark baseline, and scaling analysis |
| [`repo-clone-performance.md`](plans/repo-clone-performance.md) | Why clones are slow, with measurements and remediation options |
