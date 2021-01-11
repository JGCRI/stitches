from stitches.pkgimports import *

###############################################################################
# Helper functions to calculate Tgav from monthly netcdfs, via
# https://gallery.pangeo.io/repos/pangeo-gallery/cmip6/ECS_Gregory_method.html
###############################################################################

def get_lat_name(ds):
    """Figure out what is the latitude coordinate for each dataset."""
    for lat_name in ['lat', 'latitude']:
        if lat_name in ds.coords:
            return lat_name
    raise RuntimeError("Couldn't find a latitude coordinate")


def global_mean(ds):
    """Return global mean of a whole dataset."""
    lat = ds[get_lat_name(ds)]
    weight = np.cos(np.deg2rad(lat))
    weight /= weight.mean()
    other_dims = set(ds.dims) - {'time'}
    return (ds * weight).mean(other_dims)

###############################################################################
# Formatting the Tgav from a given file name
###############################################################################

def calc_and_format_tgav(pangeo_df_entry, save_individ_tgav = False):
    """
    From a given pangeo file path, load a netcdf file, calculate the tgav time
    series, and do some formatting to include metadata.
    
    :param pangeo_filepath:         A single pangeo_df entry, returned froma  query and corresponding to a single ensemble run.
    :param save_individ_tgav:       True/False about whether to save the individual ensemble's
    Tgav in its own file. TODO: make save path an argument.
    :return:                    A formatted data frame of Tgav time series for the
    netcdf file, including metadata.
    """
    # Load it up
    ds = xr.open_zarr(fsspec.get_mapper(pangeo_df_entry.zstore), consolidated=True)

    # some models run beyond the pandas Datetime accepted maximum in 2262. Keep
    # Only the years to 2100:
    ds = ds.sel(time=slice('1849-12-01', '2101-01-01'))
    # slice syntax is good

    # Calculate tgav
    ds_tgav = ds.pipe(global_mean).coarsen(time=12).mean()  # annual mean in each year

    # For some models, the time coordinate in the
    # xarray is a cftime.DatetimeNoLeap object. This
    # cannot be converted to a pandas datetime object.
    #
    # Obviously it is most likely that I did something
    # stupid since the pangeo cmip6 gallery example above
    # does not have this problem.
    # So, after calculating tgav,
    # the time coordinate is still a cftime.DatetimeNoLeap object,
    # just automatically filled in to July 17 (midpoint of the year).
    # we will convert to a datetime so that we can just keep the year from that.
    # It will throw a warning but in this case we don't care:
    # July 17, 1850 in a cftime object may not be the actual date July 17, 1850 as a
    # datetime. Since we only care about the year anyway, it is fine.

    # Only works when the time is a cftime.DatetimeNoLeap object. But the different
    # ESMs are using different kinds of datetime objects when they store their
    # time coordinate. Because of course.
    # ACCESS uploads theirs already as a DatetimeIndex object. So:

    # if it is already a DatetimeIndex, don't try to force to DatetimeIndex.
    # just pull off the years
    if isinstance(ds_tgav.indexes['time'], pd.DatetimeIndex):
        years = np.asarray(ds_tgav.indexes['time'].year.copy())
    else:
        # otherwise, you have to make it a DatetimeIndex first
        years = np.asarray(ds_tgav.indexes['time'].to_datetimeindex().year.copy())
    # array of integers for year values instead of datetime objects - groupby's work
    # with integers as well as datetimes, but you can't fill in missing data with integer
    # years.
    # With a datetime, you can tell it to interpolate the missing years.

    # pull off just the tgav values so we can make a new formatted table
    # of year, tgav, metadata
    tgav = ds_tgav.tas.values.copy()

    # make a pandas data frame of the years, tgav and scenario info to save as csv
    model_df = pd.DataFrame(data={'activity': pangeo_df_entry["activity_id"],
                                  'model': pangeo_df_entry["source_id"],
                                  'experiment': pangeo_df_entry["experiment_id"],
                                  'ensemble_member': pangeo_df_entry["member_id"],
                                  'timestep': pangeo_df_entry["table_id"],
                                  'grid_type': pangeo_df_entry["grid_label"],
                                  'file': pangeo_df_entry["zstore"],
                                  'year': years,
                                  'tgav': tgav})

    if save_individ_tgav:
        # save the individual model's tgav
        model_save_name = ("stitches/data/created_data/" + pangeo_df_entry["source_id"] +
                           "_" + pangeo_df_entry["experiment_id"] + "_" + pangeo_df_entry["member_id"] +
                           "_tgav.csv")
        model_df.to_csv(model_save_name)

    return model_df