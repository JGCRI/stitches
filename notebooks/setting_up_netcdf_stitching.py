# Import required libraries
import xarray as xr
import fsspec as fsspec
import pandas as pd
import numpy as np

# Define functions
#########################################################################################
def load_data(path):
    """Extract data for a single file.

        :param path:                str of the location of the cmip6 data file on pangeo.
        :return:                    an xarray containing cmip6 data downloaded from the pangeo.
    """
    ds = xr.open_zarr(fsspec.get_mapper(path))
    return ds

def get_netcdf_values(i, dl, rp, fl):

    """Extract the archive values from the list of downloaded cmip data

        :param i:              int index of the row of the recipe data frame
        :param dl:             list of xarray cmip files
        :param rp:             data frame of the recipe
        :param fl:             list of the cmip files
        :return:               a slice of xarray (not sure confident on the technical term)
    """
    file = rp["file"][i]
    start_yr = str(rp["archive_start_yr"][i])
    end_yr = str(rp["archive_end_yr"][i])

    # Figure out which index level we are on and then get the
    # xarray from the list.
    index = int(np.where(fl == file)[0])
    extracted = dl[index]
    dat = extracted.sel(time=slice(start_yr, end_yr)).tas.values.copy()

    # TODO figure out why the date range is so does not include
    expected_len = len(pd.date_range(start=start_yr+"-01-01", end=end_yr+"-12-31", freq='M'))
    assert (len(dat) == expected_len), "Not enough data in " + file + "for period " + start_yr + "-" + end_yr

    return dat


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


def stitch_gridded(rp, dl, fl):
    """stitch the gridded data together.

        :param rp:             data frame of the recipes
        :param dl:             list of the data files
        :param fl:             list of the data file names
        :return:               xarray data set

        TODO add a check to make sure that there is only one stitching id being passed into
        the function.
    """

    rp.reset_index(drop=True, inplace=True)
    out = get_netcdf_values(i=0, dl=dl, rp=rp, fl=fl)

    for i in range(1,len(rp)):
        new_vals = get_netcdf_values(i=i, dl=dl, rp=rp, fl=fl)
        out = np.concatenate((out, new_vals), axis=0)

    # Create a time series with the target data information
    # TODO this needs to be some sort of check that allows you to make sure that there is only 1 recipe being stitched
    # select monthly, daily, or annual data.
    start = str(min(rp["target_start_yr"]))
    end = str(max(rp["target_end_yr"]))

    # Note that the pd.date_range call need the date/month defined otherwise it will
    # truncate the year from start of first year to start of end year which is not
    # what we want. We want the full final year to be included in the times series.
    times = pd.date_range(start=start+"-01-01", end=end+"-12-31", freq='M')
    assert (len(out) == len(times)), "Problem with the length of time"

    # Extract the lat and lon information that will be used to structure the
    # empty netcdf file.
    lat = dl[0].lat.values
    lon = dl[0].lon.values

    # Creat the xarray data set
    rslt = xr.Dataset({'tas': xr.DataArray(
        out,
        coords=[times, lat, lon],
        dims=["time", "lat", 'lon'],
        # TODO what other meta data should be added? A path to the recepie file?
        attrs={'units': 'K',
               'stitching_id': rp['stitching_id'].unique()})
    })

    return rslt


########################################################################################
# Load the recipe for the gridded product developed from the R package.
recipe = pd.read_csv('./stitches_dev/recipes_for_python.csv')

# Save a copy of the unique file list to minimize the number of files we have to
# download from pangeo.
file_list = recipe.file.unique().copy()

# Download all of the files we need for the different recipes. This
# may take a bit of time to run and WILL NOT WORK IF CONNECTED TO VPN.
data_list = get_data_from_pangeo(file_list)

# Select a single recipe, use it to stitch gridded data to create netcdf files.
x1 = recipe[recipe['stitching_id'] == 1].copy()
out1 = stitch_gridded(rp=x1, dl=data_list, fl=file_list)
out1.to_netcdf("stitched1.nc")

x2 = recipe[recipe['stitching_id'] == 2].copy()
out2 = stitch_gridded(rp=x2, dl=data_list, fl=file_list)
out2.to_netcdf("stitched2.nc")

x3 = recipe[recipe['stitching_id'] == 3].copy()
out3 = stitch_gridded(rp=x3, dl=data_list, fl=file_list)
out3.to_netcdf("stitched3.nc")

x4 = recipe[recipe['stitching_id'] == 4].copy()
out4 = stitch_gridded(rp=x4, dl=data_list, fl=file_list)
out4.to_netcdf("stitched4.nc")