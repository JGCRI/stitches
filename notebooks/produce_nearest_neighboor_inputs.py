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

