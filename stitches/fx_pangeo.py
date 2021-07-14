# Define the functions that are useful for working with the pangeo data base
# see https://pangeo.io/index.html for more details.

import intake
import xarray as xr
import fsspec


def fetch_pangeo_table():
    """ Get a copy of the pangeo archive contents

    :return: a pd data frame containing information about the model, source, experiment, ensemble and
    so on that is available for download on pangeo.
    """

    # The url path that contains to the pangeo archive table of contents.
    url = "https://storage.googleapis.com/cmip6/pangeo-cmip6.json"
    out = intake.open_esm_datastore(url)

    return out.df


def fetch_nc(zstore):
    """Extract data for a single file.

    :param zstore:                str of the location of the cmip6 data file on pangeo.

    :return:                      an xarray containing cmip6 data downloaded from the pangeo.
    """
    ds = xr.open_zarr(fsspec.get_mapper(zstore))
    return ds
