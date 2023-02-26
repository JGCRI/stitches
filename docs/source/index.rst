Welcome to **stitches**!
--------------------------------

**stitches** is a python-based framework that provides users with a way to easily interact with StateMod in a linux-based environment. Figure 1 demonstrates the **stitches** workflow.

.. figure:: images/figure_1.jpg
  :alt: **stitches** workflow

To use **stitches**, there are a number of decisions users have to make, perhaps the most important being:

* Which ESM will **stitches** emulate?
* What archive data will be used? These are values that the target data will be matched to. It should only contain data for the specific ESM that is being emulated. Users may limit the number of experiments or ensemble realizations within the archive in order to achieve their specific experimental setup.
* What target data will be used? This data frame represents the temperature pathway the stitched product will follow. The contents of this data frame may come from CMIP6 ESM results for an SSP or it may follow some arbitrary pathway.


Why do we need **stitches**?
--------------------------------

My content here

References
---------------------------

My content here



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
