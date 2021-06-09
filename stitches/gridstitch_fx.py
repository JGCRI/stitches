# helper functions that stitch girdded data together to produce nentcdf files.

import xarray as xr
import pandas as pd
import numpy as np


#import stitches.netcdf_fx as nc
# okay for some reason python is really unhappy when it is defined in
# in the netcdf fx py script, check in with ACS and CV about why
# this might be a problem.

#def get_var_names(set):
#    """ Get the variable name from the file name inormation .
#
#        :param set:            a set of the strings describing the file names
#        :return:               a set off strings containing the cmip variable name
#    """
#    out = []
#    for text in set:
#        new = text.replace("_file", "")
#        out.append(new)
#    return out


def get_attr_info(rp, dl, fl, name):
    """Extract the cmip variable attribute information.

           :param rp:             data frame of the recepies
           :param dl:             list of the data files
           :param fl:             list of the data file names
           :param name:           string of the column containing the variable files to process
           :return:               dict object containing the cmip variable information

           TODO add a check to make sure that there is only one stitching id being passed into
           the function.
       """
    file = rp[name][0]
    index = int(np.where(fl == file)[0])
    extracted = dl[index]
    v=name.replace("_file", "")

    out=extracted[v].attrs.copy()

    return out


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

    file = rp[name][i]
    start_yr = str(rp["archive_start_yr"][i])
    end_yr = str(rp["archive_end_yr"][i])

    # Figure out which index level we are on and then get the
    # xarray from the list.
    index = int(np.where(fl == file)[0])
    extracted = dl[index]
    dat = extracted.sel(time=slice(start_yr, end_yr))[var].values.copy()

    # TODO figure out why the date range is so does not include
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

        TODO add a check to make sure that there is only one stitching id being passed into
        the function.
    """

    rp.reset_index(drop=True, inplace=True)
    # Add a for loop here that goes over the variable names....
    # but also we need some way of getting variable
    # Figure out which of the columns in the recpie data frame refer to
    # files containing cmip data
    file_column_names = rp.filter(regex='file').columns.tolist()
    # for some reason the get var name function is failing
    #var_names = get_var_names(file_column_names)
    var_names = []
    for text in file_column_names:
        new = text.replace("_file", "")
        var_names.append(new)

    # TODO wrap this in a function?
    gridded_data = []
    variable_info = []
    for name in file_column_names:

        # Save a copy of the variable attribute information and extract
        # the few years of data of data to create an array with the
        # expected structure.
        variable_info.append(get_attr_info(rp, dl, fl, name))
        out = get_netcdf_values(i=0, dl=dl, rp=rp, fl=fl, name=name)

        # Now add the
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

    # Use a for loop to fill a dictionary to hold the
    # stitched gridded data products.
    data_dict = {}
    for i in range(0, len(var_names)):
        v = var_names[i]
        d = gridded_data[i]
        a = variable_info[i]
        data_dict[v] = (["time", "lat", 'lon'], d, a)

    # Store all of the information into a xr data set, this is the final
    # object we will want to return.
    ds = xr.Dataset(
        data_vars=data_dict,
        coords=coords,
        attrs={'target data': 'Not available until full pipeline in place',
               'stitching_id': str(rp['stitching_id'].unique()),
               'recipe': 'need to figure out how to add this '}
    )

    return ds
