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

# #############################################################################
# test our anti_join function
# #############################################################################
tableA = pd.DataFrame(np.random.rand(4, 3),
                      pd.Index(list('abcd'), name='Key'),
                      ['A', 'B', 'C']).reset_index()
tableB = pd.DataFrame(np.random.rand(4, 3),
                      pd.Index(list('aecf'), name='Key'),
                      ['A', 'B', 'C']).reset_index()

print('Notice both tables have rows with Key = a, c.')
print('But different values in columns A, B, C')
print(tableA)
print(tableB)

print('Entries of tableA that do not have a Key in tableB')
print('do not care if values in ABC columns match bc bycols=Key only:')
print(util.anti_join(tableA, tableB, bycols = ['Key']))

print('Entries of tableB that do not have a Key in tableA')
print('do not care if values in ABC columns match bc bycols=Key only:')
print(util.anti_join(tableB, tableA, bycols = ['Key']))


del(tableA)
del(tableB)

# #############################################################################
# working with actual esm data and the goal functions remove_duplicates and
# the permuting recipes function
# #############################################################################


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

# Function interior
#  Intialize everything that gets updated on each iteration of the while loop:
# 1. the data frame of matched_data -> make a copy of the argument md to initialize
# 2. the data frame of duplicates
matched_data = md.copy()

# Check to see if in the matched data frame if there are any repeated values.
md_archive = matched_data[['archive_experiment', 'archive_variable', 'archive_model',
                 'archive_ensemble', 'archive_start_yr', 'archive_end_yr',
                 'archive_year', 'archive_fx', 'archive_dx']]
duplicates = matched_data.merge(md_archive[md_archive.duplicated()], how="inner")
print(duplicates)

print('so row index 0 and 2 should be rematched, targ years 1877 and 1940.')
print('and row index 1 and 3 should keep matches, targ years 1913 and 1958.')

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


# create the new archive: the old archive - all points in rm_from_archive
new_archive = util.anti_join(archive, rm_from_archive,
                   bycols=['model', 'experiment', 'variable', 'ensemble','start_yr', 'end_yr', 'year', 'fx', 'dx'])

# Find new matches for the data the target data that is missing the archive pair. Because we
# are only interested in completing our singular recipe the tol must be 0.
rematched = match.match_neighborhood(target_data=points_to_rematch, archive_data=new_archive)

print(rematched)


# Now, we update our key data frames for the next iteration of the while loop:
# 1. matched_data gets updated to be rematched + (previous matched_data minus the targets
# that were rematched).
# 2. duplicates gets recreated, checking for duplicates in our updated matched_data.

# update matched_data:
# first, drop the target windows that got rematched from the current matched_data:
matched_data_minus_rematched_targ_years = matched_data.loc[
    ~(matched_data['target_year'].isin(rematched['target_year']))].copy()

matched_data = pd.concat([matched_data_minus_rematched_targ_years, rematched])\
    .sort_values('target_year').reset_index()

# Identify duplicates in the updated matched_datafor the next iteration of the while loop
md_archive = matched_data[['archive_experiment', 'archive_variable', 'archive_model',
                           'archive_ensemble', 'archive_start_yr', 'archive_end_yr',
                           'archive_year', 'archive_fx', 'archive_dx']]
duplicates = matched_data.merge(md_archive[md_archive.duplicated()], how="inner")
print(duplicates)

del(duplicates_min, points_to_rematch, rm_from_archive, rematched,
            matched_data_minus_rematched_targ_years)

# compare to remove_duplicates output to double check:
matched_data2 = rp.remove_duplicates(md, archive)
print(matched_data.equals(matched_data2))