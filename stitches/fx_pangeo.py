# Define the functions that are useful for working with the pangeo data base
# see https://pangeo.io/index.html for more details.

import fsspec
import intake
import xarray as xr


def fetch_pangeo_table():
    """Get a copy of the pangeo archive contents

    :return: a pandas data frame containing information about the model, source, experiment, ensemble and so on that is available for download on pangeo.
    """

    # The url path that contains to the pangeo archive table of contents.
    url = "https://storage.googleapis.com/cmip6/pangeo-cmip6.json"
    out = intake.open_esm_datastore(url)

    return out.df


def fetch_nc(zstore: str):
    """Extract data for a single file.

    :param zstore:                str of the location of the cmip6 data file on pangeo.
    :type zstore:                  str

    :return:                      an xarray containing cmip6 data downloaded from  pangeo.
    """
    ds = xr.open_zarr(fsspec.get_mapper(zstore))
    ds.sortby("time")
    return ds
