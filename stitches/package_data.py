"""Module for accessing package data within the stitches package."""

import os
from importlib import resources

import xarray as xr

__all__ = ["fetch_quickstarter_data"]


def fetch_quickstarter_data(variable: str) -> xr.Dataset:
    """
    Fetch a quickstarter NetCDF dataset as an xarray object.

    :param variable: Target variable name.
    :type variable: str

    :return: Xarray Dataset for example data.
    :rtype: xr.Dataset
    """
    variable_lower = variable.casefold()

    if variable_lower not in ("tas", "pr"):
        raise KeyError(
            f"Variable '{variable}' not a valid option.  Choose from 'tas' or 'pr'."
        )

    f = (
        resources.files("stitches")
        / "data"
        / f"stitched_CanESM5_{variable_lower}_ssp245~r1i1p1f1~1.nc"
    )

    return xr.open_dataset(f)
