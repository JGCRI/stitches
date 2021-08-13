
# Import packages
import numpy as np
import pandas as pd
import xarray as xr
import os as os
import stitches.fx_util as util
import stitches.fx_data as data
import stitches.fx_pangeo as pangeo

# TODO how to make sure some functions stay internal ??
# Internal fx
def find_zfiles(rp):
    """ Determine which cmip files must be downlaoded from pangeo.
        :param rp:             data frame of the recepies
        :return:               numpy.ndarray array of the gs:// files to pull from pangeo
    """

    # Figure out which columns contain the string file
    flat_list = rp.filter(regex='file', axis=1).values.flatten()
    unique_list = np.unique(flat_list)
    return unique_list

# Internal fx
def find_var_cols(x):
    """ Determine which variables that are going to be downloaded.
        :param x:             pandas data frame of the stitches recipe
        :return:              a list of the variables that are going to be written out to the netcdf files.
    """

    # Parse out the variable name so that we can use it
    # to label the final output.
    set = x.filter(regex='file').columns.tolist()

    out = []
    for text in set:
        new = text.replace("_file", "")
        out.append(new)
    return out

# Internal fx
def get_netcdf_values(i, dl, rp, fl, name):

    """Extract the archive values from the list of downloaded cmip data
        :param i:              int index of the row of the recipe data frame
        :param dl:             list of xarray cmip files
        :param rp:             data frame of the recipe
        :param fl:             list of the cmip files
        :param name:           name of the variable file that is going to be processed.
        :return:               a slice of xarray (not sure confident on the technical term)
    """

    file = rp[name][i]
    start_yr = str(rp["archive_start_yr"][i])
    end_yr = str(rp["archive_end_yr"][i])

    # Figure out which index level we are on and then get the
    # xarray from the list.
    index = int(np.where(fl == file)[0])
    extracted = dl[index].sortby('time')
    v=name.replace("_file", "")
    # TODO CRV why is this function not working any more???
    dat = extracted.sel(time=slice(start_yr, end_yr))[v].values.copy()

    # TODO figure out why the date range is so does not include
    expected_len = len(pd.date_range(start=start_yr+"-01-01", end=end_yr+"-12-31", freq='M'))
    assert (len(dat) == expected_len), "Not enough data in " + file + "for period " + start_yr + "-" + end_yr

    return dat

# Internal
def get_var_info(rp, dl, fl, name):
    """Extract the cmip variable attribute information.
               :param rp:             data frame of the recipes
               :param dl:             list of the data files
               :param fl:             list of the data file names
               :param name:           string of the column containing the variable file name from rp
               :return:               pandas dataframe of the variable meta data
               TODO add a check to make sure that there is only one stitching id being passed into
               the function.
           """
    util.check_columns(rp, {name})
    file = rp[name][0]
    index = int(np.where(fl == file)[0])
    extracted = dl[index]

    attrs = data.get_ds_meta(extracted)
    return attrs

# Internal
def get_atts(rp, dl, fl, name):
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

# Internal
def internal_stitch(rp, dl, fl):
    """Stitch a single recpie into netcdf outputs
        :param dl:             list of xarray cmip files
        :param rp:             data frame of the recipe
        :param fl:             list of the cmip files
        :return:               a list of the data arrays for the stitched products of the different variables.
    """

    rp.reset_index(drop=True, inplace=True)
    vars=find_var_cols(rp)
    out = []

    # For each of the of the variables stitch the
    # data together.
    for v in vars:

        # Get the information about the variable that is going to be stitched together.
        col = v + '_file'
        var_info = get_var_info(rp, dl, fl, col)

        # For each of time slices extract the data & concatenate together.
        gridded_data = get_netcdf_values(i=0, dl=dl, rp=rp, fl=fl, name=col)

        # Now add the other time slices.
        for i in range(1, len(rp)):
            new_vals = get_netcdf_values(i=i, dl=dl, rp=rp, fl=fl, name=col)
            gridded_data = np.concatenate((gridded_data, new_vals), axis=0)

        # Note that the pd.date_range call need the date/month defined otherwise it will
        # truncate the year from start of first year to start of end year which is not
        # what we want. We want the full final year to be included in the times series.
        # TODO need to add ability to control the temporal resolution of the target data.
        start = str(min(rp["target_start_yr"]))
        end = str(max(rp["target_end_yr"]))

        if var_info["frequency"][0].lower() == "mon":
            freq = "M"
        else:
            raise TypeError(f"unsupported frequency")

        times = pd.date_range(start=start + "-01-01", end=end + "-12-31", freq=freq)
        assert (len(gridded_data) == len(times)), "Problem with the length of time"

        # Extract the lat and lon information that will be used to structure the
        # empty netcdf file. Make sure to copy all of the information including
        # the attributes!
        lat = dl[0].lat.copy()
        lon = dl[0].lon.copy()

        r = rp.reset_index(drop=True).to_string()
        rslt = xr.Dataset({v: xr.DataArray(
            gridded_data,
            coords=[times, lat, lon],
            dims=["time", "lat", 'lon'],
            attrs={'units': var_info['units'][0],
                   'variable': var_info['variable'][0],
                   'experiment': var_info['experiment'][0],
                   'ensemble': var_info['ensemble'][0],
                   'model': var_info['model'][0],
                   'stitching_id': rp['stitching_id'].unique()[0],
                   'recipe': r})
        })

        out.append(rslt)

    out_dict = dict(zip(vars, out))

    return out_dict


# Function to export
# TODO figure out the best way to wrap this or apply it mulitple recpies
# TODO also need to figure out a better way to do the naming...
def stitching(out_dir, rp):
    """Stitch
        :param out_dir:        string directory location where to write the netcdf files out to
        :param rp:             data frame of the recipe
        :return:               a list of the netcdf files paths
    """

    flag = os.path.isdir(out_dir)
    if flag == False:
        raise TypeError(f'The output directory does not exist.')

    # Check inputs.
    util.check_columns(rp, {'target_start_yr', 'target_end_yr', 'archive_experiment',
                            'archive_variable', 'archive_model', 'archive_ensemble', 'stitching_id',
                            'archive_start_yr', 'archive_end_yr'})

    # Determine which variables will be downloaded.
    vars = find_var_cols(rp)
    if not (len(vars) >= 1):
        raise TypeError(f'No variables were found to be processed.')

    # Determine which files need to be downloaded from pangeo.
    file_list = find_zfiles(rp)

    # Download all of the data from pangeo.
    # TODO figure out how to optmize this, run time increases with the # of files trying to fetch
    # TODO This will not work with VPN do we need to add some sort of check?
    map_dl = map(pangeo.fetch_nc, file_list)
    data_list = list(map_dl)

    # Do the stitching!
    rslt = internal_stitch(rp, data_list, file_list)

    # Print the files out at netcdf files
    f = []
    for i in rslt.keys():
        ds = rslt[i]
        fname = "./stitched_" + ds[i].attrs['model'] + '_' + ds[i].attrs['variable'] + '_' + ds[i].attrs['stitching_id'] + '.nc'
        ds.to_netcdf(fname)
        f.append(fname)

    return f









