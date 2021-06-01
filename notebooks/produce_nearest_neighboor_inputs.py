# This script generates the chunked data that will be used in the nearest neighbor
# exploration, which for expediency purposes will be done within R check out the
# stitches/notebooks/stitches_dev for the R project and associated files

# set up the python environment, note that for now all of the paths are
# hard coded! You will need to update the variable `local_location`
# with where the stitches project lives on your computer

from stitches.pkgimports import *
import stitches.dev_matchup as matchup
import pandas as pd

# relative pathnames don't work when just running a python script.
# Create a string with the local location of the stitches directory
local_location = "/Users/snyd535/Documents/task11a-topdown-clim-ML/stitches"
# local_location = "/Users/dorh012/Documents/2021/stitches"


# Read in all of the smoothed esm data
smoothed_data_path = local_location + "/stitches/data/created_data/main_smooth_tgav_anomaly_all_pangeo_list_models.dat"
smoothed_data = pd.read_pickle(smoothed_data_path)

#list the model names of interest
model_names = ["ACCESS-ESM1-5",
                "CanESM5",
                "GISS-E2-1-G",
                "MIROC6",
                "MPI-ESM1-2-HR",
                "MPI-ESM1-2-LR",
                "NorCPM1",
                "UKESM1-0-LL"]

for name in model_names:

    archive_smooth = smoothed_data[smoothed_data["model"] == name].copy()
    data = archive_smooth[["model", "experiment", "ensemble", "year", "variable","value"]].copy()
    group_by = ['model', 'experiment', 'ensemble', 'variable']
    # For each group in the data set go through, chunk and extract the fx and dx␣ values.
    # For now we have to do this with a for loop, to process each model/experiment/ensemble/variable
    # individually.
    out = []
    for key, d in data.groupby(group_by):
        # make sure only operate as long as we don't have a full
        # time series of all nan
        if not (np.isnan(d["value"]).all()):
            rslt = matchup.get_chunk_info(matchup.chunk_ts(df=d, n=9))
            out.append(rslt)
        # end if statement
    # end of the for loop

    final = pd.concat(out, ignore_index=True)
    final.to_csv(local_location + "/notebooks/stitches_dev/inputs/" + name + "_archive_data.csv", index = False)
# end the for loop over ESMs
