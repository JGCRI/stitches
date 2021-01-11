# Not a notebook since community version of pycharm does
# not offer jupyter lab integration

# Load all of the libraries
from matplotlib import pyplot as plt
import xarray as xr
import numpy as np
import pandas as pd
import dask
from dask.diagnostics import progress
from tqdm.autonotebook import tqdm
import intake
import fsspec
import seaborn as sns
import sys
from collections import defaultdict
import nc_time_axis
import fsspec
# libraries need for pattern scaling:
from sklearn.linear_model import LinearRegression
import itertools
import datetime

import stitches

import stitches.readpangeo as read

###############################################################################
# Get a first cut list of models.
###############################################################################

# open up the full pangeo set of data
pangeo = intake.open_esm_datastore("https://storage.googleapis.com/cmip6/pangeo-cmip6.json")

# define experiments of interest
expts = ['ssp126', 'ssp245', 'ssp370', 'ssp585', 'ssp119', 'ssp434',
         'ssp460', 'ssp534-over', "historical"]
# only take models that have daily data; if they have daily, they almost certainly
# have monthly. Daily netcdfs are less commonly submitted, so doing the search on
# only day will probably return fewer models for our proof of concept + then we
# can validate on both monthly and daily data.
table_ids = ['day']

# Get the ensemble/experiment counts
tas_table = read.create_count_table(pangeo, "tas",
                                    experiments=expts,
                                    table_ids=table_ids,
                                    min_ensemble_size=5)
pr_table = read.create_count_table(pangeo, "pr",
                                   experiments=expts,
                                   table_ids=table_ids,
                                   min_ensemble_size=5)

# Combine the tas and pr counts into a single object
tas_table.columns = ['source_id', 'tas_exp', 'tas_mem']
pr_table.columns = ['source_id', 'pr_exp', 'pr_mem']

count_table = tas_table.merge(pr_table, on="source_id", how="left")

# Print the full list of models to process
print('Models to process')
print(count_table["source_id"].values)

del (tas_table)
del (pr_table)


###############################################################################
# Turn the list of experiments and the models in count_table into a query to
# get a pangeo subsetted list of files that we want to loop over.
###############################################################################

# CMIP6 models don't submit annual netcdfs, so we will calculate
# tgav from monthly data.
query = dict(
    experiment_id=expts,
    variable_id='tas',
    source_id=count_table["source_id"].copy(),
    table_id='Amon',
    grid_label='gn'
)

# Subset the pangeo catalog with our query.
# This is the master list of files that we want to calculate Tgav across.
# Except it includes more than one physics setting so we have to subset further.
pangeo_subset = pangeo.search(**query)

del (count_table)
###############################################################################
# Subset to only inclue the p1 settings.
# I can't figure out how to form the query to make sure
# we return all ensemble members but only for the p1
# settings. So we will further subset our query results
# to do this because it is easier for how much python
# I know right now.
###############################################################################

# Convert to a pandas data frame now that we are just
# down to a table of names of files that we want
# to manipulate so that we can load the corresponding
# netcdf via xarray
df = pangeo_subset.df.copy()  # rest of code appears to work even without
# explicitly turning into pandas data frame
# with pd.DataFrame(pangeo_subset_day.df)
###NNN
# df.loc()
# also lambda statements within applys (unnamed functions) and xarray applys.

df['filter_id'] = pd.Series(df.member_id).str.contains('p1').tolist()

# do the filter
df1 = df[df['filter_id'] == True]

# drop the filter_id column
df1 = df1.drop('filter_id', 1, inplace=True)
# inplace = True saves us from having to do .copy and overwriting -
# df1.drop('filter_id', 1) will change what gets displayed, but
#  doesn't affect df1.


# and overwrite what is in our original name
pangeo_subset.df = df1.copy()

# and delete our intermediate data frames created
# out of an abundance of caution and to keep things
# separated/clearer to me while doing the filter
del (df1)
del (df)

###############################################################################
# Summarize how many files we have to process when all is said and done:
###############################################################################

# pangeo_subset.df is now our master list of files to calculate Tgav for each.

# Find how many ensemble members each model did for each experiment
print('Number of files to process:')
print(pangeo_subset.df.groupby("source_id")[["activity_id", "experiment_id", "member_id"]].nunique())
print('---------------------------------------------------------------')
print('Total Number files:')
print(pangeo_subset.df['source_id'].count())

###############################################################################
# Load and process all of these.
# Doing it in serial - it will take a long time but I can do it with what I know
# now and it will work.
###############################################################################

# we're going to save this as a master csv file instead of netcdfs because we
# just don't need that structure and the associated memory footprint.
pangeo_df = pangeo_subset.df.copy()
pangeo_df = pangeo_df[["activity_id", "source_id", "experiment_id",
                       "member_id", "table_id", "variable_id",
                       "grid_label", "zstore"]]

# save the pangeo list metadata
pangeo_df.to_csv("created_data/pangeo_file_list_for_tgav.csv")
# are some cases where pickle-ing to_pickle() lets you save
# more structured files for ingesting back in later so that it
# comes back in as the exact data structure (like saving as RDS)

# Initialize empty holder
all_tgav = pd.DataFrame()

for file_index in [0]:  # list(range(0, len(pangeo_df) + 1)):

    # get the file path one at a time from the master list pangeo_subset
    file_path = pangeo_df.zstore.values[file_index]

    # Load it up
    ds = xr.open_zarr(fsspec.get_mapper(file_path), consolidated=True)

    # some models run beyond the pandas Datetime accepted maximum in 2262. Keep
    # Only the years to 2100:
    ds = ds.sel(time=slice('1849-12-01', '2101-01-01'))
    # slice syntax is good

    # Calculate tgav
    ds_tgav = ds.pipe(global_mean).coarsen(time=12).mean()  # annual mean in each year
    # why .pipe(fcn) instead of fcn(ds)

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

    tgav = ds_tgav.tas.values.copy()

    del (ds)
    del (ds_tgav)

    # make a pandas data frame of the years, tgav and scenario info to save as csv
    model_df = pd.DataFrame(data={'activity': pangeo_df["activity_id"].values[file_index],
                                  'model': pangeo_df["source_id"].values[file_index],
                                  'experiment': pangeo_df["experiment_id"].values[file_index],
                                  'ensemble_member': pangeo_df["member_id"].values[file_index],
                                  'timestep': pangeo_df["table_id"].values[file_index],
                                  'grid_type': pangeo_df["grid_label"].values[file_index],
                                  'year': years,
                                  'tgav': tgav,
                                  'file': pangeo_df["zstore"].values[file_index]})
    # save the individual model's tgav
    model_save_name = ("created_data/" + pangeo_df["source_id"].values[file_index] +
                       "_" + pangeo_df["experiment_id"].values[file_index] + "_" + pangeo_df["member_id"].values[
                           file_index] +
                       "_tgav.csv")

    model_df.to_csv(model_save_name)

    # and append to the full list
    all_tgav = all_tgav.append(model_df)

    # clean up
    del (model_df)
    del (model_save_name)
    del (tgav)
    del (years)
    del (file_path)

# end for loop over file index

# save the full file of tgav values

all_tgav.to_csv('created_data/main_tgav_all_pangeo_list_models.csv')