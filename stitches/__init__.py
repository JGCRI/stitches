"""
The __init__.py file for the stitches package.

The stitches package provides tools for stitching together climate model output
into a single, coherent dataset.
"""


from ._version import __version__
from .fx_match import match_neighborhood
from .fx_pangeo import fetch_nc, fetch_pangeo_table
from .fx_recipe import generate_gridded_recipe, make_recipe, permute_stitching_recipes
from .fx_stitch import gmat_stitching, gridded_stitching
from .generate_package_data import generate_pkg_data
from .install_pkgdata import install_package_data
from .make_matching_archive import make_matching_archive
from .make_pangeo_table import make_pangeo_comparison, make_pangeo_table
from .make_tas_archive import make_tas_archive
from .package_data import fetch_quickstarter_data

__all__ = [
    "match_neighborhood",
    "fetch_nc",
    "fetch_pangeo_table",
    "generate_gridded_recipe",
    "make_recipe",
    "permute_stitching_recipes",
    "gmat_stitching",
    "gridded_stitching",
    "generate_pkg_data",
    "install_package_data",
    "make_matching_archive",
    "make_pangeo_comparison",
    "make_pangeo_table",
    "make_tas_archive",
    "fetch_quickstarter_data",
    "__version__",
]
