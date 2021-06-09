# Helper functions designed to help access data from pangeo. Note these functions
# will not work if connected to vpn.

# load libraries
import xarray as xr
import fsspec as fsspec
import numpy as np


def load_data(path):
    """Extract data for a single file.

        :param path:                str of the location of the cmip6 data file on pangeo.
        :return:                    an xarray containing cmip6 data downloaded from the pangeo.
    """
    ds = xr.open_zarr(fsspec.get_mapper(path))
    ds = ds.sortby("time")
    return ds

def get_data_from_pangeo(fl):
    """Download all of the data from a pangeo file list

        :param fl:             list of the files to get data from
        :return:               list of xarray
    """
    data_list = []

    # this for loop is really really freaking slow need to figure out how
    # the heck to do this also only want to have to run it once runing it mulitple
    # times is awuful!
    for f in fl:
        o = load_data(f)
        data_list.append(o)

    return data_list

def find_zfiles(rp):
    """ Determine which cmip files must be downlaoded from pangeo.

        :param rp:             data frame of the recepies
        :return:               numpy.ndarray array of the gs:// files to pull from pangeo
    """

    # Figure out which columns contain the string file
    flat_list = rp.filter(regex='file', axis=1).values.flatten()
    unique_list = np.unique(flat_list)
    return unique_list