Welcome to **stitches**!
--------------------------------

**stitches** is a python-based framework that provides users with a way to easily interact with Pangeo-hosted CMIP6 data to emulate the output variables of a target Earth System Model (ESM). Figure 1 demonstrates the **stitches** workflow.

.. figure:: images/figure_1.jpg
  :alt: **stitches** workflow

To use **stitches**, there are a number of decisions users have to make, perhaps the most important being:

* Which ESM will **stitches** emulate?
* What archive data will be used? These are values that the target data will be matched to. It should only contain data for the specific ESM that is being emulated. Users may limit the number of experiments or ensemble realizations within the archive in order to achieve their specific experimental setup.
* What target data will be used? This data frame represents the global average temperature pathway the stitched product will follow. The contents of this data frame may come from CMIP6 ESM results for an SSP or it may follow some arbitrary pathway.


Why do we need **stitches**?
--------------------------------

Impact research often requires many output variables from ESMs, including but not limited to global gridded temperature, precipitation, sea level pressure, relative humidity, and more. Impact research also often requires these values on a monthly or even daily time scale. 

ESMs are expensive to run, often limiting the scenarios that can be explored and the number of ensemble members that can be generated for each scenario. **stitches** intelligently recombines the existing model runs available from ESMs in CMIP6 into global, gridded, multivariate outputs for *novel scenarios* on monthly or daily timescales. **stitches** can also be used to enrich the ensemble sizes of existing scenarios. The resulting generated gridded multivariate outputs preserve the ensemble statistics of the ESM's data. 



References
---------------------------

Tebaldi et al. "STITCHES: creating new scenarios of climate model output by stitching together pieces of existing simulations"
*Earth System Dynamics* , 2022.
https://doi.org/10.5194/esd-13-1557-2022




Documentation
--------------------------

.. toctree::
   :maxdepth: 1
   :caption: Getting Started

   getting-started/installation
   getting-started/quickstarter
   getting-started/tutorial

.. toctree::
   :maxdepth: 1
   :caption: User Guides

   examples/modify-inputs

.. toctree::
   :maxdepth: 1
   :caption: Python API

   reference/api

.. toctree::
   :maxdepth: 1
   :caption: Contributing

   reference/contributing
