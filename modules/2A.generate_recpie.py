# This is where the recpie is made based on matching nearest neighboor
# It is a transaltion of the stitches_dev R work.
# TODO My guess is that eventually this may be combined with the level 3 module.

# #############################################################################
# General setup
# #############################################################################
# Import packages
import pickle_utils as pickle
import stitches.fx_match as match
import stitches.fx_recepie as recipe


data=pickle.load("stitches/data/matching_archive.pkl", compression="zip")

target_data = data[data["model"] == 'CanESM5']
target_data = target_data[target_data["experiment"] == 'ssp245']
target_data = target_data[target_data["ensemble"].isin(['r4i1p1f1', 'r1i1p1f1'])]
target_data = target_data.reset_index(drop=True)

archive_data = data[data['model'] == 'CanESM5'].copy()

# Due to some quirk related to how march neighborhood is written (not functioning as a closed
# environment, do python functions not do that?) the match_neighborhood need to read in
# fresh target & archive data. This is unideal behavior that should be fixed.
test = match.match_neighborhood(target_data, archive_data, tol=0.1)

# So the permute stitiching recipe works it works when N matches is set to 1 see blow for an
# example where the function fails to return 2 matches per target.
unformatted_recipe = recipe.permute_stitching_recipes(N_matches=1, matched_data=test, archive=archive_data)

# Problematic behavior
# Errors are thrown when N matches is not set to 1, unclear what is going on it is returning
# a single recipe and rejecting all of the proposed new recipes. I suspect that this has to
# do with how the matched data frame is being updated. Because I think that it is just spitting
# out the same recipe over and over again.
# out2 = recipe.permute_stitching_recipes(N_matches=4, matched_data=test, archive=archive_data)


# The next step is to clean up the recipe so that it can be used to generate the gridded data products.

test = recipe.handle_transition_periods(unformatted_recipe)
