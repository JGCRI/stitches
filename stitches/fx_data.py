"""Module for working with temperature data files."""

import numpy as np
import pandas as pd


def get_lat_name(ds):
    """Get the name for the latitude values in an xarray dataset.

    This function searches for latitude coordinates in the dataset,
    which could be named either 'lat' or 'latitude'.

    :param ds: The dataset from which to retrieve the latitude coordinate name.
    :type ds: xarray.Dataset
    :returns: The name of the latitude variable.
    :rtype: str
    :raises RuntimeError: If no latitude coordinate is found in the dataset.
    """
    for lat_name in ["lat", "latitude"]:
        if lat_name in ds.coords:
            return lat_name
    raise RuntimeError("Couldn't find a latitude coordinate")


def global_mean(ds):
    """
    Calculate the weighted global mean for a variable in an xarray dataset.

    :param ds: The xarray dataset of CMIP data.
    :type ds: xarray.Dataset
    :returns: The xarray dataset of the weighted global mean.
    :rtype: xarray.Dataset
    """
    lat = ds[get_lat_name(ds)]
    weight = np.cos(np.deg2rad(lat))
    weight /= weight.mean()
    other_dims = set(ds.dims) - {"time"}
    return (ds * weight).mean(other_dims)


def get_ds_meta(ds):
    """
    Get the metadata information from an xarray dataset.

    :param ds: xarray dataset of CMIP data.
    :return: pandas DataFrame of MIP information.
    """
    v = ds.variable_id

    data = [
        {
            "variable": v,
            "experiment": ds.experiment_id,
            "units": ds[v].attrs["units"],
            "frequency": ds.attrs["frequency"],
            "ensemble": ds.attrs["variant_label"],
            "model": ds.source_id,
        }
    ]
    df = pd.DataFrame(data)

    return df
