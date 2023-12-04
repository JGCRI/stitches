---
title: "STITCHES: a Python package to amalgamate existing Earth system model output into new scenario realizations"
tags:
  - Python
  - earth system models
  - emulation
  - internal variability
authors:
  - name: Abigail C. Snyder
    orcid: 0000-0002-9034-9948
    affiliation: 1
  - name: Kalyn R. Dorheim
    orcid: 0000-0001-8093-8397
    affiliation: 1
  - name: Claudia Tebaldi
    orcid: 0000-0001-9233-8903
    affiliation: 1
  - name: Chris R. Vernon
    orcid: 0000-0002-3406-6214
    affiliation: 2
affiliations:
  - name: Joint Global Change Research Institute/Pacific Northwest National Laboratory, USA
    index: 1
  - name: Pacific Northwest National Laboratory, USA
    index: 2
date:
bibliography: paper.bib

---

# Statement of need

State of the art impact models characterizing aspects of the interaction between
the human and Earth systems require decade-long time series of relatively high
frequency, spatially resolved and often multiple variables representing
climatic impact-drivers. Most commonly these are derived from Earth System
Model (ESM) output, according to a standard, limited set of future scenarios,
the latest being the SSP-RCPs run under CMIP6-ScenarioMIP [@Eyringetal2016;@ONeilletal2016].
Often, however, impact modeling seeks to explore new scenarios, and/or needs a
larger set of initial condition ensemble members than are typically available to
quantify the effects of ESM internal variability. In addition, the recognition that
the human and Earth systems are fundamentally intertwined, and may feature
potentially significant feedback loops, is making integrated, simultaneous modeling
of the coupled human-Earth system increasingly necessary, if computationally
challenging [@thornton2017biospheric].
For the dual needs of the creation of new scenario realizations and the
simplified representation of ESM behavior in a coupled human-Earth system
modeling framework, climate model output emulators can be the answer.
We proposed a new, comprehensive approach to such emulation, STITCHES [@tebaldi2022stitches].
The corresponding `stitches` Python package uses existing archives of ESMs’
scenario experiments to construct new scenarios, or enrich existing initial
condition ensembles. Its output has the same characteristics of the ESM output
emulated: multivariate (spanning potentially all variables that the ESM has
saved), spatially resolved (down to the native grid of the ESM), and as high
frequency as the original output has been saved at.


# Summary

ESM emulation methods generally attempt to preserve the complex statistical
characteristics of ESM outputs for multiple variables and at time scales (often
daily or monthly) relevant to impacts models.
While many existing ESM emulation methods rely on 'bottom up' methods (inferring
the multivariate distribution governing the spatiotemporal behavior of ESM
outputs), `stitches` instead takes a top-down approach more similar to the
warming-level style of analyses used by past Intergovernmental Panel on Climate
Change reports [@SR15]. Specifically, `stitches` takes existing ESM
output and intelligently recombines time windows in these gridded, multivariate
outputs into new instances by stitching them together on the basis of a target
global average temperature (GSAT) trajectory representing an existing or new
scenario, as long as the latter is intermediate to existing ones in forcing levels.
Research from the climate science
community has indicated that many ESM output variables are tightly dependent upon
the GSAT trajectory and thus scenario independent, justifying our approach.
Thus, the statistical characteristics of ESM
output are preserved by the construction process. Variables that represent
the cumulative effect of warming, such as sea level rise, cannot be emulated
directly with `stitches`, but a multitude of impact-relevant variables can be.


The distributions inferred via bottom-up methods can often be used to generate
an unlimited number of realizations, however the emulators trained with bottom-
up methods often can only handle a small number of variables jointly (e.g. temperature and precipitation). By contrast, `stitches` can produce new
realizations for any variables archived by the ESM, albeit finitely many new
realizations dependent on the number of runs archived by each ESM. For example,
global hydrology models often require at least monthly temperature, precipitation,
and humidity variables to run, with additional variables often ideal. Currently,
new realizations can be appended to archived ESM realizations to result in
double to triple the number of runs available (depending on variables of
interest), with plans in future versions to further increase this generated
ensemble size.

The `stitches` Python package currently relies on close integration with the
Pangeo cloud catalog of CMIP6 ESM outputs
(https://gallery.pangeo.io/repos/pangeo-gallery/cmip6/). Thanks to
this integration, users are not required to pre-download the entire CMIP6-ScenarioMIP
archive of ESM outputs, and can quickly and flexibly
emulate variables from any of the 40 ESMs participating in ScenarioMIP.
In addition to the requirements for working with Pangeo in Python,
`stitches` relies only on a few common scientific Python packages
(`xarray`, `numpy`, `pandas`, `sk-learn`), which are specified required dependencies
in the package. Finally, because `stitches` is intended for use by
impacts modelers, the
new realizations generated by `stitches` are NetCDF files with the same
dimension information and generally identical structure to the original CMIP6
ESM outputs. These outputs from `stitches` can then serve as inputs to impacts
models with little to no code changes in the impacts models.


# Code availability
The `stitches` GitHub repository (https://github.com/JGCRI/stitches) provides
installation instructions.

Also included is a [quickstart notebook](https://github.com/JGCRI/stitches/blob/main/notebooks/stitches-quickstart.ipynb) that serves as a tutorial for using the package.


# Acknowledgements

This research was supported by the U.S. Department of Energy, Office of Science, as part of research in MultiSector Dynamics, Earth and Environmental System Modeling Program.

# References
