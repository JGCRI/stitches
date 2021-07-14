# Create the archive of the temperature value & rate of change, these
# are the values that are used in the matching process.
# TODO is there some way to increase the number of points in the matching archive?

# #############################################################################
# General setup
# #############################################################################
# Import packages
import pickle_utils as pickle
import pandas as pd
import stitches.fx_processing as prep

# Heuristically determined on acs-smoothing-investigation branch
smoothing_window = 9

# Heuristically determined on acs-smoothing-investigation branch
chunk_window = 9

# Have pandas print all columns in console
pd.set_option('display.max_columns', None)

# Load the tas data
data=pickle.load("stitches/data/tas_values.pkl", compression="zip")

# #############################################################################
# 1. Get the value & rate of change per chunk
# #############################################################################
# Smooth the anomalies, get the running mean of each time series.
smoothed_data=prep.calculate_rolling_mean(data, smoothing_window)

# For each group in the data set go through, chunk and extract the fx and dx␣ values.
# For now we have to do this with a for loop, to process each model/experiment/ensemble/variable
# individually.
data = smoothed_data[["model", "experiment", "ensemble", "year", "variable","value"]]
group_by = ['model', 'experiment', 'ensemble', 'variable']
out = []
for key, d in data.groupby(group_by):
    dd = prep.chunk_ts(df=d, n=9)
    rslt = prep.get_chunk_info(dd)
    out.append(rslt)
# end of the for loop & concatenate results into a single data frame.
data = pd.concat(out).reset_index()

# Save the results, this is archive we will do the matching on.
pickle.dump(data, "stitches/data/matching_archive.pkl", compression="zip")
# Optional save as a csv file if need to
if False:
    data.to_csv("stitches/data/matching_archive.csv", index=False)

# #############################################################################
# 2. Visualize Archive
# #############################################################################
if False:
    from plotnine import *

    (ggplot() +
     geom_point(aes(x="fx", y="dx", color="model"), data=data, size=1, alpha=0.4) +
     labs(title="Value vs Rate of Change", y="dx", x="fx") +
     theme_bw()
     )

    (ggplot() +
     geom_point(aes(x="fx", y="dx"), data=data, size=1, alpha=0.4) +
     labs(title="Value vs Rate of Change", y="dx", x="fx") +
     facet_wrap("model", scales="free") +
     theme_bw()
     )
