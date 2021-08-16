# debugging remove_duplicates and the permutation function.

# using csv versions of files to avoid pickle-utils dependency right now

# This is meant to be executed line by line. Works otherwise, but the print statements
# make less sense.

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

# Here we load the archive.
data = pd.read_csv("stitches/data/matching_archive.csv").drop(['index'], axis=1).copy()


# Select the some data to use as the target data
target_data = data[data["model"] == 'CanESM5'].copy()
target_data = target_data[target_data["experiment"] == 'ssp245']
target_data = target_data[target_data["ensemble"].isin(['r1i1p1f1'])]
target_data = target_data.reset_index(drop=True)

# Select the data to use as our archive.
archive_data = data[data['model'] == 'CanESM5'].copy()


# #############################################################################
# debug remove_duplicates
# #############################################################################

# go through the interior of fx_recipe.py > remove_duplicates()

# Initialize the arguments to remove_duplicates
md = match.match_neighborhood(target_data, archive_data, tol=0.0)
# ^ this does happen to have duplicates:
archive = archive_data.copy()



# meatof the function
md_archive = md[['archive_experiment', 'archive_variable', 'archive_model',
                     'archive_ensemble', 'archive_start_yr', 'archive_end_yr',
                     'archive_year', 'archive_fx', 'archive_dx']]
duplicates = md.merge(md_archive[md_archive.duplicated()], how="inner")
print(duplicates)


# In the while loop - iterate manually:
#
# within each iteration of checking duplicates,
# pull out the one with smallest dist_l2 -
# this is the one that gets to keep the match, and we use
# as an index to work on the complement of (in case the same
# archive point gets matched for more than 2 target years)
grouped = duplicates.groupby(['archive_experiment', 'archive_variable',
                              'archive_model', 'archive_ensemble',
                              'archive_start_yr', 'archive_end_yr', 'archive_year',
                              'archive_fx', 'archive_dx'])
# Pick which of the target points will continue to be matched with the archive
# pair.
dat = []
for name, group in grouped:
    min_value = min(group['dist_l2'])
    dat.append(group.loc[group['dist_l2'] == min_value])
duplicates_min = pd.concat(dat)
print(duplicates_min)


# target points of duplicates-duplicates_min need to be
# refit on the archive, the  ones that need a new pairing.
filter_col = [col for col in duplicates if col.startswith('target_')]
points_to_rematch = duplicates[filter_col].loc[(~duplicates['target_year'].isin(duplicates_min['target_year']))]
new_names = list(map(lambda x: x.replace('target_', ''), points_to_rematch.columns))
points_to_rematch.columns = new_names


# Because we know that none of the archive values can be reused in the match discard them
# from the updated archive that will be used in the rematching.
cols = [col for col in md if col.startswith('archive_')]
rm_from_archive = md[cols]
new_names = list(map(lambda x: x.replace('archive_', ''), rm_from_archive.columns))
rm_from_archive.columns = new_names


