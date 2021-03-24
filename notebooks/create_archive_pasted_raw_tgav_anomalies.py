# Not a notebook since community version of pycharm does
# not offer jupyter lab integration.

# This takes the main tgav file previously created, renames,
# converts to anomalies, and pastes the appropriate historical data
# into every ssp run.

# * Names subject to change.

# TODO update the way we handle where things are getting saved.



# #############################################################################
## General setup
# #############################################################################
from stitches.pkgimports import *

# Load the matchup related functions.
import stitches.dev_matchup as matchup


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

# paste the historical data into every future scenario

t_paste = matchup.paste_historical_data(input_data = t_anomaly)

# save
with open("stitches/data/created_data/main_raw_pasted_tgav_anomaly_all_pangeo_list_models.dat", 'wb') as f:
    # Pickle the 'data' dictionary using the highest protocol available.
    pickle.dump(t_paste, f, pickle.HIGHEST_PROTOCOL)

t_paste.to_csv("stitches/data/created_data/main_raw_pasted_tgav_anomaly_all_pangeo_list_models.csv")