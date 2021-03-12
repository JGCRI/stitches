# Not a notebook since community version of pycharm does
# not offer jupyter lab integration.

# This takes the main tgav file previously created, renames,
# converts to anomalies, and smooths to create a main_smooth_tgav_anomaly
# file.

# * Names subject to change.

# TODO update the way we handle where things are getting saved

# #############################################################################
## General setup
# #############################################################################
from stitches.pkgimports import *

# Load the matchup related functions.
import stitches.dev_matchup as matchup

smoothing_window = 9 # heuristically determined on acs-smoothing-investigation branch

# have pandas print all columns in console
pd.set_option('display.max_columns', None)

# #############################################################################
## Import data, calculate anomaly, smooth, save
# #############################################################################
# Import the data and select the model to use, I suspect that in the future these will be
# combined into a single function call.
data = matchup.cleanup_main_tgav("./stitches/data/created_data/main_tgav_all_pangeo_list_models.dat")

# Calculate the temperature anomaly relative to 1995 - 2014 (IPCC reference period).
t_anomaly = matchup.calculate_anomaly(data).drop('activity', 1)

# smooth the anomalies
smoothed_t_anomaly = matchup.calculate_rolling_mean(t_anomaly, smoothing_window)

# save
with open("stitches/data/created_data/main_smooth_tgav_anomaly_all_pangeo_list_models.dat", 'wb') as f:
    # Pickle the 'data' dictionary using the highest protocol available.
    pickle.dump(smoothed_t_anomaly, f, pickle.HIGHEST_PROTOCOL)
