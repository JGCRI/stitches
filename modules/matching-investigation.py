# Investigating the runs that had a single spurious stitched window in the
# scenarioMIP experiment.
#
# Hypothesis 1: direct result of the matching - thinking the target window w/ the
# spurious match was one that only had a single match in the nn+tol neighborhood
# and that another target window matched to the same archive point with a smaller
# distance. So the target window w/ the spurious match got rematched to its
# nearest neighbor in the subsetted Archive-MatchedPts (regardless of tolerance,
# as the algorithm dictates so that we always have at least one match). IE there just
# isn't a good, full nearest-neighborgh-without-repetition match possible for these
# couple ESMs, likely because they submitted very few ensemble members to CMIP6.
# (and in the scenarioMIP experiment, that becomes even fewer).
# So this is what we're testing here, by stepping through the interior of our
# matching function.
#
# Hypothesis 2: tolerance is just a hair too high - investigate only if find
# hypothesis 1 is wrong.
#
# Using SSP126 and SSP585 as bracketing scenarios,
# emulate  SSP245 and SSP370 as target scenarios.

# #############################################################################
# General setup
# #############################################################################
# Import packages
import pandas as pd
import numpy as np
import stitches as stitches
import stitches.fx_util as util
import stitches.fx_match as match
import pkg_resources

pd.set_option('display.max_columns', None)

OUTPUT_DIR = pkg_resources.resource_filename('stitches', 'data/created_data')

# #############################################################################
# Experiment  setup
# #############################################################################
# experiment parameters
tolerance = 0.075
Ndraws = 1

esms = ['BCC-CSM2-MR',  'MCM-UA-1-0', 'NorESM2-LM', 'NorESM2-MM',
        'NESM3', 'TaiESM1']

# 'BCC-CSM2-MR', ssp245
#  'MCM-UA-1-0', ssp245 and ssp 370
# 'NESM3', ssp245  ***
# 'NorESM2-MM', ssp245 and ssp 370
# 'NorESM2-LM', ssp370
# 'TaiESM1', ssp 370


# #############################################################################
# The experiment
# #############################################################################

# Load the full archive of all staggered windows, which we will be matching on
full_archive_path = pkg_resources.resource_filename('stitches', 'data/matching_archive.csv')
full_archive_data = pd.read_csv(full_archive_path)

# Load the original archive without staggered windows, which we will draw
# the target trajectories from for matching
full_target_path = pkg_resources.resource_filename('stitches', 'data/matching_archive.csv')
full_target_data = pd.read_csv(full_target_path)


# for each of the esms in the experiment, subset to what we want
# to work with and run the experiment.
archive_out = []
targets_out = []
checking_out = []
for esm in esms:
    print(esm)

    # subset the archive and the targets to this ESM
    archive_data = full_archive_data[(full_archive_data['model'] == esm) &
                                     ((full_archive_data['experiment'] == 'ssp126') |
                                      (full_archive_data['experiment'] == 'ssp585'))].copy()
    archive_out.append(archive_data[['model', 'experiment', 'ensemble']].drop_duplicates())

    target_245 = full_target_data[(full_target_data['model'] == esm) &
                                  (full_target_data['experiment'] == 'ssp245')].copy()

    target_370 = full_target_data[(full_target_data['model'] == esm) &
                                  (full_target_data['experiment'] == 'ssp370')].copy()
    targets_out.append(target_245[['model', 'experiment', 'ensemble']].drop_duplicates())
    targets_out.append(target_370[['model', 'experiment', 'ensemble']].drop_duplicates())


    # replacing duplicate matches is handled in the permutation code, not the matching code.
    # so get the matches:
    if not target_245.empty:
        match245 = match.match_neighborhood(target_245, archive_data, tol=tolerance)
        # take a look at the places where the issue is probably coming in:
        check245 = match245[['target_model', 'target_variable', 'target_experiment',
                             'target_ensemble', 'target_year', 'archive_experiment',
                             'archive_ensemble', 'archive_year', 'dist_l2']].drop_duplicates().copy()
        checking_out.append(check245[check245['target_year'] > 2040])

    if not target_370.empty:
        match370 = match.match_neighborhood(target_370, archive_data, tol=tolerance)
        check370 = match370[['target_model', 'target_variable', 'target_experiment',
                             'target_ensemble', 'target_year', 'archive_experiment',
                             'archive_ensemble', 'archive_year', 'dist_l2']].drop_duplicates().copy()
        checking_out.append(check370[check370['target_year'] > 2030])

# end for loop over ESMs

archive_out =  pd.concat(archive_out).reset_index(drop=True).copy()
archive_out.to_csv((OUTPUT_DIR +'/matching_investigation_hypothesis1/scenarioMIP_archive_sizes.csv'), index=False)

targets_out =  pd.concat(targets_out).reset_index(drop=True).copy()
targets_out.to_csv((OUTPUT_DIR +'/matching_investigation_hypothesis1/scenarioMIP_target_sizes.csv'), index=False)

checking_out =  pd.concat(checking_out).reset_index(drop=True).copy()
checking_out.to_csv((OUTPUT_DIR +'/matching_investigation_hypothesis1/scenarioMIP_checking_matches.csv'), index=False)

