# debugging permute_stitching_recipes

# using csv versions of files to avoid pickle-utils dependency right now

# This is meant to be executed line by line, checking that every step of
# the permute_stitching_recipes function does what it is supposed to on sample data.
# Also temporary, easy to remove once confident.

# #############################################################################
# General setup
# #############################################################################
# Import packages
import pandas as pd
import numpy as np
import stitches.fx_util as util
import stitches.fx_match as match
import stitches.fx_recepie as rp

pd.set_option('display.max_columns',21)


''# #############################################################################
# working with actual esm data and permute_stitching_recipes
# #############################################################################

# Here we load the archive.
data = pd.read_csv("stitches/data/matching_archive.csv").drop(['index'], axis=1).copy()


# Select the some data to use as the target data
target_data = data[data["model"] == 'CanESM5'].copy()
target_data = target_data[target_data["experiment"] == 'ssp245']
target_data = target_data[target_data["ensemble"].isin(['r1i1p1f1', 'r4i1p1f1'])]
target_data = target_data.reset_index(drop=True)

# Select the data to use as our archive.
archive_data = data[data['model'] == 'CanESM5'].copy()


# #############################################################################
# work out the interior beh
# #############################################################################

# go through the interior of fx_recipe.py > permute_stitching_recipes()

# Initialize the arguments to remove_duplicates. Small tol so that we do
# get multiple matches to test behavior but not massive data frames to
# work with.
matched_data = match.match_neighborhood(target_data, archive_data, tol=0.01)
archive = archive_data.copy()
N_matches = 2

print(matched_data)
x = rp.get_num_perms(matched_data)
print(x[0])
# so r1 can support 2 collapse free generated Tgavs, r4 can support 1.
# We will get either 2 or 3 generated total, depending on what the
# matches actually are.



# #############################################################################
# Function Interior
# #############################################################################


# Initialize quantities updated on every iteration of the while loop:
# 1. A copy of the matched data
# 2. perm_guide

# Initialize matched_data_int for iteration through the while loop:
# make a copy of the data to work with to be sure we don't touch original argument
matched_data_int = matched_data.drop_duplicates().copy()

# identifying how many target windows are in a trajectory we want to
# create so that we know we have created a full trajectory with no
# missing widnows; basically a reference for us to us in checks.
num_target_windows = util.nrow(matched_data_int["target_year"].unique())
print(num_target_windows) # should be 28


# Initialize perm_guide for iteration through the while loop.
# the permutation guide is one of the factors that the while loop
# will run checks on, must be initialized.
# Perm_guide is basically a dataframe where each target window
# lists the number of archive matches it has.
num_perms = rp.get_num_perms(matched_data_int)
perm_guide = num_perms[1]


# how many target trajectories are we matching to,
# how many collapse-free ensemble members can each
# target support, and order them according to that
# for construction.
targets = num_perms[0].sort_values(["minNumMatches"]).reset_index()
# Add a column of a target  id name, differentiate between the different input
# streams we are emulating.
# We specifically emulate starting with the realization that can support
# the fewest collapse-free generated realizations and work in increasing
# order from there. We iterate over the different realizations to facilitate
# checking for duplicates across generated realizations across target
# realizations.
targets['target_ordered_id'] = ['A' + str(x) for x in targets.index]
print(targets)


# intialize the empty data frame to hold the total collection of recipes
recipe_collection = pd.DataFrame()


# Loop over each target ensemble member, creating N_matches generated
# realizations via a while loop before moving to the next target.



# #############################################################################
# first iteration of for loop over targets/first target:
# #############################################################################
target_id = 'A0'
# subset the target info, the target df contains meta information about the run we
# and the number of permutations and such.
target = targets.loc[targets["target_ordered_id"] == target_id].copy()

# initialize a recipes data frame holder for each target, for
# the while loop to iterate on
recipes_col_by_target = pd.DataFrame()
var_name = target['target_variable'].unique()[0]
exp = target['target_experiment'].unique()[0]
mod = target['target_model'].unique()[0]
ens = target['target_ensemble'].unique()[0]

# While the following conditions are met continue to generate new recipes.
# 1. While we have fewer matches than requested for the target ensemble_member,
#    keep going.
# 2. Filter the perm_guide to just the target ensemble member in this loop and
#    make sure there are at least num_target_windows of time windows: basically
#    make sure there is at least one remaining archive match to draw from for
#    each target window in this target ensemble. Note this means the perm_guide
#    must be updated at the end of every while loop iteration.

# Intialize these conditions so we enter the while loop, then update again at the
# end of each iteration
if util.nrow(recipes_col_by_target) == 0:
    condition1 = True
elif util.nrow(recipes_col_by_target['stitching_id'].unique()) < N_matches:
    condition1 = True
else:
    condition1 = False

perm_rows = util.nrow(
    perm_guide.loc[(perm_guide['target_variable'] == var_name) & (perm_guide['target_experiment'] == exp) &
                   (perm_guide['target_model'] == mod) & (perm_guide['target_ensemble'] == ens)]
        .copy()
        .drop_duplicates())

if perm_rows == num_target_windows:
    condition2 = True
else:
    condition2 = False

print(condition1)
print(condition2)
print(all([condition1, condition2]))

# And an integer index to initialize the count of stitched
# trajectories for each target
stitch_ind = 1

# first (should be only) iteration of while loop for A0


# Group matched data for a single target by the chunks of the target information.
# Right now a single target chunk may have multiple matches with archive points. The
# next several steps of the while loop will create a one to one paring between the
# target and archive data, then check to make sure that the pairing meets the requirements
# for what we call a recipe.
grouped_targets = matched_data_int.loc[(matched_data_int['target_variable'] == var_name) &
                                       (matched_data_int['target_experiment'] == exp) &
                                       (matched_data_int['target_model'] == mod) &
                                       (matched_data_int['target_ensemble'] == ens)].copy().groupby(
    ['target_variable', 'target_experiment', 'target_ensemble', 'target_model',
     'target_start_yr', 'target_end_yr'])

# For each target window group,
# Randomly select one of the archive matches to use
one_one_match = []
for name, group in grouped_targets:
    one_one_match.append(group.sample(1, replace=False))
one_one_match = pd.concat(one_one_match)
one_one_match = one_one_match.reset_index()


# Before we can accept our candidate recipe, one_one_match,
# we run it through a lot of tests.

# Force one_one_match to meet our first condition,
# that each archive data point in the recipe must be unique.
# Then give it a stitching id
new_recipe = rp.remove_duplicates(one_one_match, archive)
stitching_id = exp + '~' + ens + '~' + str(stitch_ind)
new_recipe["stitching_id"] = stitching_id
new_recipe = new_recipe.drop(['index'], axis = 1).copy()


# Compare the new_recipe to the previously drawn recipes across all target
# ensembles.
# There is no collapse within each target ensemble because  we remove the constructed
# new_recipe from the matched_data at the end of each iteration of the while loop -
# The sampled points CAN'T be used again for the current target ensemble member
# for loop iteration.
# The code below is ensuring there is no collapse across
# the previously generated trajectories for previous target ensemble members.
# Again, the challenge is seeing if our entire sample has
# been included in recipes before, not just a row or two.

if util.nrow(recipe_collection) != 0:
    # If previous recipes exist, we must create a comparison
    # data frame that checks each existing recipe in recipe_collection
    # against new_recipe and record True/False
    #
    # Compare the new recipe with the existing collection of all recipes.
    # We only care about the actual (target_window, archive_window) pairs,
    # so we can just compare those character columns.
    cols_to_use = ['target_variable', 'target_experiment', 'target_ensemble',
                   'target_model', 'target_start_yr', 'target_end_yr', 'archive_experiment',
                   'archive_variable', 'archive_model', 'archive_ensemble', 'archive_start_yr',
                   'archive_end_yr']
    grouped_collection = recipe_collection.groupby(['stitching_id'])
    comparison = []
    for name, group in grouped_collection:
        df1 = group[cols_to_use].copy().reset_index(drop=True).sort_index(axis=1)
        df2 = new_recipe[cols_to_use].copy().reset_index(drop=True).sort_index(axis=1)
        comparison.append(all(df1 == df2))

        # end for loop
    # end if statement
else:
    # Otherwise, this is the first recipe we've done at all, so we set comparison manually
    # so that the next if statement triggers just like it was appending a new recipe to an
    # existing list.
    comparison = [False]
# end else

# If the new_recipe is not unique (aka, any(comparison) == True), then
# we don't want it and we don't want to do anything else in this iteration of
# the while loop. We DON'T update the matched_points or conditions, so the
# while loop is forced to re-run so that another random draw is done to create
# a new candidate new_recipe.
# We check for what we want: the new recipe is the first or all(comparsion)==False.
# In either case, we are safe to keep new_recipe and update all the data frames
# for the next iteration of the while loop.
if  all(comparison) == False:

    # add new_recipe to the list of recipes for this target ensemble
    recipes_col_by_target = pd.concat([recipes_col_by_target, new_recipe]).copy()

    # And we remove it from the matched_points_int so the archive
    # values used in this new_recipe can't be used to construct
    # subsequent realizations for this target ensemble member.
    # This updated matched_data_int is used in each iteration
    # of the while loop. Since we are removing the constructed
    # new_recipe from the matched_data_int at the end of each
    # iteration of the while loop, the sample points can't be
    # randomly drawn again for the next generated trajectory
    # of the current target ensemble member for loop iteration.

    # Now each (target_window, archive_window) combination must
    # be removed from matched data for all target ensemble members,
    # not just the one we are currently operating on.
    # Use an anti-join
    matched_data_int = util.anti_join(matched_data_int, new_recipe.drop(['stitching_id'], axis = 1).copy(),
                                      bycols=["target_year", "target_start_yr", "target_end_yr",
                                              "archive_experiment", "archive_variable", "archive_model",
                                              "archive_ensemble", "archive_start_yr", "archive_end_yr",
                                              "archive_year"]).copy()


    # update permutation count info with the revised matched data so
    # the while loop behaves - this makes sure that every target window
    # in the perm_guide actually has at least one matched archive point
    # available for draws .
    # That way, we don't try to construct a trajectory with fewer years
    # than the targets.
    num_perms = rp.get_num_perms(matched_data_int)
    perm_guide = num_perms[1]


    # Use the updated perm_guide to update
    # the while loop conditions:

    # Condition 1:
    # If we haven't reached the N_matches goal for this target ensemble
    if util.nrow(recipes_col_by_target) == 0:
        condition1 = True
    elif util.nrow(recipes_col_by_target['stitching_id'].unique()) < N_matches:
        condition1 = True
    else:
        condition1 = False
    # end Condition 1

    # Condition 2:
    # make sure each target window in the updated perm guide has at least one archive match available
    # to draw on the next iteration.
    perm_rows = util.nrow(
        perm_guide.loc[
            (perm_guide['target_variable'] == var_name) & (perm_guide['target_experiment'] == exp) &
            (perm_guide['target_model'] == mod) & (perm_guide['target_ensemble'] == ens)]
            .copy()
            .drop_duplicates())

    if perm_rows == num_target_windows:
        condition2 = True
    else:
        condition2 = False
    # end updating condition 2

    # Add to the stitch_ind, to update the count of stitched
    # trajectories for each target ensemble member.
    stitch_ind += 1

    # end if statement
# end the while loop for this target ensemble member
# In debugging, there's only one being constructed so our condition2 should be false now
print(condition2)

# Add the recipes created for this ensemble member to the entire collection
# of recipes.
recipe_collection = recipe_collection.append(recipes_col_by_target)




