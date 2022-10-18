# IN DEVELOPMENT!

 [![DOI](https://zenodo.org/badge/317969428.svg)](https://zenodo.org/badge/latestdoi/317969428) ![test-coverage](https://github.com/JGCRI/stitches/workflows/build/badge.svg) [![codecov](https://codecov.io/gh/JGCRI/stitches/branch/main/graph/badge.svg)](https://codecov.io/gh/JGCRI/stitches) [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/JGCRI/stitches.git/main?filepath=notebooks%2Fsample.ipynb)

# stitches
Amalgamate existing climate data to create monthly climate variable fields.

## Getting Started Using `stitches`
Jupyter notebooks hosted on `stitches` use functionality that is contained within the accompanying Python package.  

**NOTE:**  Ensure you are using Python >= 3.9.  Calling `python` may use a different instance.  Some users may need to use `python3` or the like instead.

`stitches`  users and developers must agree to our community guidelines outlines in our community guidelines outlines in our 
[contirbutors guidelines](CONTRIBUTING.md) and [code of conduct](CODE_OF_CONDUCT.md). 
Open an issue to ask for help or report an issue ([how to open a github issue](https://docs.github.com/en/enterprise-server@3.1/issues/tracking-your-work-with-issues/creating-an-issue)). 

#### For developers
Clone the repository and then install the package in development mode using the following from inside the package's root directory:
```bash
python setup.py develop
```

Before `stitches` can be used, the internal package data must be installed or
generated.

To install pre-built run the following:
```bash
import stitches
stitches.install_package_data()
```

For users that would like to generate the package data directly, run the following:

```bash
import stitches 
stitches.generate_pkg_data()
```
but note that this will take several hours to run.


#### For users
To install for use, run the following:
```bash
python -m pip install git+https://github.com/JGCRI/stitches.git
```

To install pre-built run the following:
```bash
import stitches
stitches.install_package_data()
```

For users that would like to generate the run the following: 

```bash
import stitches 
stitches.generate_pkg_data()
```
but note that this will take several hours to run.




## Tutorial Jupyter Notebooks
### Sample
| Notebook | Description |
|:-:|:-:|
| `stitches-quickstart.ipynb` | Simple tutorial to demonstrate how `stitches` can be used as an emulator. |
