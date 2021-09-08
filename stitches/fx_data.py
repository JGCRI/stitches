# Define the collection of functions that are useful for working with the
# temperature data files.

import numpy as np
import pandas as pd

def get_lat_name(ds):
    """ Get the name for the latitude values (could be either lat or latitude).

    :param ds:    xarray dataset of CMIP data.

    :return:    the string name for the latitude variable.
    """
    for lat_name in ['lat', 'latitude']:
        if lat_name in ds.coords:
            return lat_name
    raise RuntimeError("Couldn't find a latitude coordinate")

def global_mean(ds):
    """ Get the weighted global mean for a variable.

    :param ds:  xarray dataset of CMIP data.

    :return:    xarray dataset of the weighted global mean.
    """
    lat = ds[get_lat_name(ds)]
    weight = np.cos(np.deg2rad(lat))
    weight /= weight.mean()
    other_dims = set(ds.dims) - {'time'}
    return (ds * weight).mean(other_dims)

def get_ds_meta(ds):
    """ Get the meta data information from the xarray data set.

    :param ds:  xarray dataset of CMIP data.

    :return:    pandas dataset of MIP information.
    """
    v = ds.variable_id

    data = [{'variable':v,
             'experiment':ds.experiment_id,
             'units':ds[v].attrs['units'],
            'frequency': ds.attrs["frequency"],
             'ensemble':ds.attrs["variant_label"],
             'model': ds.source_id}]
    df = pd.DataFrame(data)

    return df

