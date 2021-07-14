# helper functions that stitch girdded data together to produce nentcdf files.

import xarray as xr
import pandas as pd
import numpy as np
import stitches.netcdf_fx as nc


def get_netcdf_values(i, dl, rp, fl, name):
    """Extract the archive values from the list of downloaded cmip data

        :param i:              int index of the row of the recipe data frame
        :param dl:             list of xarray cmip files
        :param rp:             data frame of the recipe
        :param fl:             list of the cmip files
        :param name:           the name of the column containing the files we want to process
        :return:               a slice of xarray (not sure confident on the technical term)
    """
    # Make sure the file name we are working with is defined in the rp data frame.
    assert (sum(rp.columns == name) == 1), "file name not found in rp"
    var = name.replace("_file", "")  # parse out the variable name from the file_column name

    assert (rp.stitching_id.unique().__len__() == 1), "only 1 stitching recepie can be read in at a time"


    file = rp[name][i]
    start_yr = str(rp["archive_start_yr"][i])
    end_yr = str(rp["archive_end_yr"][i])

    # Figure out which index level we are on and then get the
    # xarray from the list.
    index = int(np.where(fl == file)[0])
    extracted = dl[index]
    dat = extracted.sel(time=slice(start_yr, end_yr))[var].values.copy()

    expected_len = len(pd.date_range(start=start_yr + "-01-01", end=end_yr + "-12-31", freq='M'))
    assert (len(dat) == expected_len), "Not enough data in " + file + "for period " + start_yr + "-" + end_yr

    return dat


def stitch_gridded(rp, dl, fl):
    """stitch the gridded data together.

        :param rp:             data frame of the recepies, may contain any number of variables
            so long as the column containing the variable has the following nomenclature "var_file"
        :param dl:             list of the data files
        :param fl:             list of the data file names
        :return:               xarray data set

        TODO add some capability to determine where to write the files out to

    """

    rp.reset_index(drop=True, inplace=True)
    # Add a for loop here that goes over the variable names....
    # but also we need some way of getting variable
    # Figure out which of the columns in the recpie data frame refer to
    # files containing cmip data
    file_column_names = rp.filter(regex='file').columns.tolist()
    # for some reason the get var name function is failing
    var_names = nc.get_var_names(file_column_names)


    # TODO wrap this in a function?
    gridded_data = []
    variable_info = []
    for name in file_column_names:

        # Save a copy of the variable attribute information and extract
        # the few years of data of data to create an array with the
        # expected structure.
        variable_info.append(nc.get_attr_info(rp, dl, fl, name))
        out = get_netcdf_values(i=0, dl=dl, rp=rp, fl=fl, name=name)

        # Now add the other time slices.
        for i in range(1, len(rp)):
            new_vals = get_netcdf_values(i=i, dl=dl, rp=rp, fl=fl, name=name)
            out = np.concatenate((out, new_vals), axis=0)

        # Make an array of the gridded data products
        gridded_data.append(out)

    # Create a time series with the target data information
    # select monthly, daily, or annual data.
    start = str(min(rp["target_start_yr"]))
    end = str(max(rp["target_end_yr"]))

    # Note that the pd.date_range call need the date/month defined otherwise it will
    # truncate the year from start of first year to start of end year which is not
    # what we want. We want the full final year to be included in the times series.
    times = pd.date_range(start=start + "-01-01", end=end + "-12-31", freq='M')
    assert (len(out) == len(times)), "Problem with the length of time"

    # Extract the lat and lon information that will be used to structure the
    # empty netcdf file. Make sure to copy all of the information including
    # the attributes!
    lat = dl[0].lat.copy()
    lon = dl[0].lon.copy()
    coords = dict(time=times,
                  lat=lat,
                  lon=lon)

    # Use a for loop to go create and save a netcdf file
    # for each of the variables.
    f = []
    for i in range(0, len(var_names)):
        data_dict = {}
        v = var_names[i]
        d = gridded_data[i]
        a = variable_info[i]
        data_dict[v] = (["time", "lat", 'lon'], d, a)

        # Store all of the information into a xr data set, this is the final
        # object we will want to return.
        r = rp.reset_index(drop=True).to_string()
        ds = xr.Dataset(
            data_vars=data_dict,
            coords=coords,
            attrs={'target data': 'Not available until full pipeline in place',
                   'stitching_id': str(rp['stitching_id'].unique()[0]),
                   'recipe': r,
                   'variable':v})

        m = str(rp.source_id.unique()[0])
        # TODO there should be some way to figure out how way control how to write it out!
        fname = "./stitched_" + m + '_' + v + '_' + str(rp['stitching_id'].unique()[0]) + '.nc'
        ds.to_netcdf(fname)
        f.append(fname)

    return f