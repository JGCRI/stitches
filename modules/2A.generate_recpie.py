# This is where the recpie is made based on matching nearest neighboor
# It is a transaltion of the stitches_dev R work.
# TODO My guess is that eventually this may be combined with the level 3 module.

# #############################################################################
# General setup
# #############################################################################
# Import packages
import pickle_utils as pickle
import stitches.fx_match as match

data=pickle.load("stitches/data/matching_archive.pkl", compression="zip")

target_data = data[data["model"] == 'NorESM2-LM']
target_data = target_data[target_data["experiment"] == 'ssp460']
target_data = target_data[target_data["ensemble"] == 'r1i1p1f1']
target_data = target_data.reset_index(drop=True)

archive_data = data.copy()

test = match.match_neighborhood(target_data, archive_data, tol = 0.1)