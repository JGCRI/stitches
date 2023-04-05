import os
import pkg_resources

import xarray as xr


def fetch_quickstarter_data(variable: str) -> xr.Dataset:
    """Get a quickstarter NetCDF dataset as an xarray object.

    :param variable:                Target variable name.
    :type variable:                 str

    :return:                        Xarray Dataset for example data
    :rtype:                         xr.Dataset

    """
    variable_lower = variable.casefold()

    if variable_lower not in ("tas", "pr"):
        raise KeyError(f"Variable '{variable}' not a valid option.  Choose from 'tas' or 'pr'.")

    data_directory = pkg_resources.resource_filename("stitches", "data")
    filename = f"stitched_CanESM5_{variable_lower}_ssp245~r1i1p1f1~1.nc"

    return xr.open_dataset(os.path.join(data_directory, filename))
