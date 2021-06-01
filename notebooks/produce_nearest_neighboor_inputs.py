# This script generates the chunked data that will be used in the nearest neighbor
# exploration, which for expediency purposes will be done within R check out the
# stitches/notebooks/stitches_dev for the R project and associated files

# set up the python environment, note that for now all of the paths are
# hard coded!

from stitches.pkgimports import *
import stitches.dev_matchup as matchup
import pandas as pd


# Read in all of the smoothed esm data
smoothed_data_path = "./stitches/data/created_data/main_smooth_tgav_anomaly_all_pangeo_list_models.dat"
smoothed_data = pd.read_pickle(smoothed_data_path)

# Subset the data so that it contains a single realization of a middle of the road
# scenario for a model. This is going to be the time series we want to emulate aka
# our the target_smoothed_tgav_timeseries from (https://gcims.atlassian.net/wiki/spaces/HOME/pages/669220883/stitches+design+document)
target_smooth = smoothed_data[smoothed_data["model"] == "CanESM5"].copy()
target_smooth = target_smooth[target_smooth["experiment"] == "ssp245"].copy()
target_smooth = target_smooth[target_smooth["ensemble"] == "r1i1p1f1"].copy()
target_smooth = target_smooth[["model", "experiment", "ensemble", "year", "variable", "value"]]

# This is our target data that we will use in the nearest neighbor to match on
target_data = matchup.get_chunk_info(matchup.chunk_ts(target_smooth, 9))
target_data.to_csv("/Users/dorh012/Documents/2021/stitches/notebooks/stitches_dev/inputs/target_data.csv", index = False)

# Now calculate the data from the archive, this is what the target data will be
# matched on. Here we calculate the chunk data from ALL of the CanESM time series,
# then we can subset what data we want to use in the nearest neighbor matching
# exploration analysis we will do in R.

# Read in all of the smoothed esm data again, need to make sure that all of data is being based
# into the archive.
smoothed_data_path = "/Users/dorh012/Documents/2021/stitches/stitches/data/created_data/main_smooth_tgav_anomaly_all_pangeo_list_models.dat"
smoothed_data = pd.read_pickle(smoothed_data_path)

archive_smooth = smoothed_data[smoothed_data["model"] == "CanESM5"].copy()
data = archive_smooth[["model", "experiment", "ensemble", "year", "variable","value"]].copy()
group_by = ['model', 'experiment', 'ensemble', 'variable']
# For each group in the data set go through, chunk and extract the fx and dx␣ values.
# For now we have to do this with a for loop, to process each model/experiment/ensemble/variable
# individually.
out = []
for key, d in data.groupby(group_by):
    rslt = matchup.get_chunk_info(matchup.chunk_ts(df=d, n=9))
    out.append(rslt)
# end of the for loop

final = pd.concat(out, ignore_index=True)
final.to_csv("/Users/dorh012/Documents/2021/stitches/notebooks/stitches_dev/inputs/archive_data.csv", index = False)

# save a csv file version of the global tgav that can be used in the example analysis that is going on in R
tgav_data_path = "/Users/dorh012/Documents/2021/stitches/stitches/data/created_data/main_tgav_all_pangeo_list_models.dat"
tgav_data = pd.read_pickle(tgav_data_path)
tgav_data.columns

# rename the columns of the tgav data so that we can read it into the calculate anomlay function
# 'activity', 'model', 'experiment', 'ensemble', 'timestep', 'grid_type',
#         'file', 'year', 'value', 'variable'

# Format the data frame to be used by the calculate_anomaly function
data = matchup.cleanup_main_tgav("./stitches/data/created_data/main_tgav_all_pangeo_list_models.dat")

# Calculate the temperature anomaly relative to 1995 - 2014 (IPCC reference period).
t_anomaly = matchup.calculate_anomaly(data).drop('activity', 1)
t_anomaly.to_csv("/Users/dorh012/Documents/2021/stitches/notebooks/stitches_dev/inputs/tgav_data.csv", index = False)


