# Derive optimal tolerance by ESM-SSP and  well-characterized errors
# for an audience seeking to emulate from the full CMIP6 archive,
# for a selection of ESMs.

# For each ESM, loop over various tolerances and generate Ndraws = 500
# GSAT trajectories. Archives with and without each target to characterize
# error. (Reproducible mode off so draws are different).
# Compare to the target ensemble via 4 metrics (E1, E2 on both Tgavs and
# jumps) to select the optimal tol for each ESM-SSP-archive combo.
# Use the resultant optimal tol for a final, reproducible mode set of recipes,
# Tgavs, and gridded data sets of monthly + daily for ??? variables.
# Start with tas, pr, psl - must subset archive up front to only include
# ensemble members that have all of these files available.
# look at CanESM, MIROC6 and ACCESS

## TODO functionalize at least some part of the analysis or at least use a for-loop
## over the different SSP targets so that the code isn't so long and repetitive

# would be better to functionalize this script with ESM, tol and Ndraws as arguments
# and then have the .sh just call the function and dispatch to diff nodes for each run I guess.

# #############################################################################
# General setup
# #############################################################################
# Import packages
import pandas as pd
import numpy as np
import stitches as stitches
import pkg_resources
import os
from  pathlib import Path

pd.set_option('display.max_columns', None)

OUTPUT_DIR = pkg_resources.resource_filename('stitches', 'data/created_data')
# OUTPUT_DIR = '/pic/projects/GCAM/stitches_pic/paper1_outputs'

# #############################################################################
# Experiment  setup
# #############################################################################
# experiment parameters
tolerances = np.arange(0.05, 0.3, 0.005)
Ndraws = 2
error_threshold = 0.1

# pangeo table of ESMs for reference
pangeo_path = pkg_resources.resource_filename('stitches', 'data/pangeo_table.csv')
pangeo_data = pd.read_csv(pangeo_path)
pangeo_data = pangeo_data[((pangeo_data['variable'] == 'tas') | (pangeo_data['variable'] == 'pr') | (pangeo_data['variable'] == 'psl'))
                          & ((pangeo_data['domain'] == 'Amon') | (pangeo_data['domain'] == 'day')) ].copy()

# Keep only the runs that have data for all vars X all timesteps:
pangeo_good_ensembles =[]
for name, group in pangeo_data.groupby(['model', 'experiment', 'ensemble']):
    df = group.drop_duplicates().copy()
    if len(df) == 6:
        pangeo_good_ensembles.append(df)
    del(df)
pangeo_good_ensembles = pd.concat(pangeo_good_ensembles)
pangeo_good_ensembles  = pangeo_good_ensembles[['model', 'experiment', 'ensemble']].drop_duplicates().copy()
pangeo_good_ensembles = pangeo_good_ensembles.reset_index(drop=True).copy()

# won't use idealized runs
pangeo_good_ensembles = pangeo_good_ensembles[~((pangeo_good_ensembles['experiment'] == '1pctCO2') |
                                                (pangeo_good_ensembles['experiment'] == 'abrupt-4xCO2')) ].reset_index(drop=True).copy()

esms = ['ACCESS-CM2', 'CanESM5', 'MIROC6']
# ['ACCESS-CM2', 'ACCESS-ESM1-5', 'AWI-CM-1-1-MR', 'BCC-CSM2-MR',
#        'BCC-ESM1', 'CESM2', 'CESM2-FV2', 'CESM2-WACCM', 'CMCC-CM2-HR4',
#        'CMCC-CM2-SR5', 'CMCC-ESM2', 'CanESM5', 'HadGEM3-GC31-LL',
#        'HadGEM3-GC31-MM', 'IITM-ESM', 'MIROC-ES2L', 'MIROC6',
#        'MPI-ESM-1-2-HAM', 'MPI-ESM1-2-HR', 'MPI-ESM1-2-LR', 'MRI-ESM2-0',
#        'NorESM2-LM', 'NorESM2-MM', 'SAM0-UNICON', 'TaiESM1',
#        'UKESM1-0-LL']


# #############################################################################
# Load full archive and target data
# #############################################################################

# Load the full archive of all staggered windows, which we will be matching on
full_archive_path = pkg_resources.resource_filename('stitches', 'data/matching_archive_staggered.csv')
full_archive_data = pd.read_csv(full_archive_path)

# Keep only the entries that appeared in pangeo_good_ensembles:
keys =['model', 'experiment', 'ensemble']
i1 = full_archive_data.set_index(keys).index
i2 = pangeo_good_ensembles.set_index(keys).index
full_archive_data= full_archive_data[i1.isin(i2)].copy()
del(i1)
del(i2)

# Load the original archive without staggered windows, which we will draw
# the target trajectories from for matching
full_target_path = pkg_resources.resource_filename('stitches', 'data/matching_archive.csv')
full_target_data = pd.read_csv(full_target_path)

# Keep only the entries that appeared in pangeo_good_ensembles:
keys =['model', 'experiment', 'ensemble']
i1 = full_target_data.set_index(keys).index
i2 = pangeo_good_ensembles.set_index(keys).index
full_target_data = full_target_data[i1.isin(i2)].copy()
del(i1)
del(i2)
del(keys)

# #############################################################################
# Some helper functions
# #############################################################################
def get_jumps(tgav_df):
    tgav_jump = []
    for name, group in tgav_df.groupby(['variable', 'experiment', 'ensemble', 'model']):
        ds = group.copy()
        ds['jump'] = ds.value.diff().copy()
        ds = ds.dropna().copy()
        tgav_jump.append(ds)
        del (ds)
    tgav_jump = pd.concat(tgav_jump)
    tgav_jump = tgav_jump.drop_duplicates().reset_index(drop=True).copy()
    return(tgav_jump)

def four_errors(gen_data, orig_data):
    gen_data_jump = get_jumps(gen_data)
    orig_data_jump = get_jumps(orig_data)

    orig_stats = []
    for name, group in orig_data.groupby(['model', 'variable', 'experiment']):
        ds = group.copy()
        ds1 = ds[['model', 'variable', 'experiment']].drop_duplicates().copy()
        ds1['mean_orig_tgav'] = np.mean(ds.value.values)
        ds1['sd_orig_tgav'] = np.std(ds.value.values)
        orig_stats.append(ds1)
        del (ds)
        del (ds1)
    orig_stats = pd.concat(orig_stats).reset_index(drop=True).copy()

    orig_stats_jump = []
    for name, group in orig_data_jump.groupby(['model', 'variable', 'experiment']):
        ds = group.copy()
        ds1 = ds[['model', 'variable', 'experiment']].drop_duplicates().copy()
        ds1['mean_orig_jump'] = np.mean(ds.value.values)
        ds1['sd_orig_jump'] = np.std(ds.value.values)
        orig_stats_jump.append(ds1)
        del (ds)
        del (ds1)
    orig_stats_jump = pd.concat(orig_stats_jump).reset_index(drop=True).copy()

    orig_stats = orig_stats.merge(orig_stats_jump, how='left', on=['model', 'variable', 'experiment']).copy()
    del (orig_stats_jump)

    gen_stats = []
    for name, group in gen_data.groupby(['model', 'variable', 'experiment', 'tolerance', 'draw', 'archive']):
        ds = group.copy()
        ds1 = ds[['model', 'variable', 'experiment', 'tolerance', 'draw', 'archive']].drop_duplicates().copy()
        ds1['mean_gen_tgav'] = np.mean(ds.value.values)
        ds1['sd_gen_tgav'] = np.std(ds.value.values)
        gen_stats.append(ds1)
        del (ds)
        del (ds1)
    gen_stats = pd.concat(gen_stats).reset_index(drop=True).copy()

    gen_stats_jump = []
    for name, group in gen_data_jump.groupby(['model', 'variable', 'experiment', 'tolerance', 'draw', 'archive']):
        ds = group.copy()
        ds1 = ds[['model', 'variable', 'experiment', 'tolerance', 'draw', 'archive']].drop_duplicates().copy()
        ds1['mean_gen_jump'] = np.mean(ds.value.values)
        ds1['sd_gen_jump'] = np.std(ds.value.values)
        gen_stats_jump.append(ds1)
        del (ds)
        del (ds1)
    gen_stats_jump = pd.concat(gen_stats_jump).reset_index(drop=True).copy()

    gen_stats = gen_stats.merge(gen_stats_jump, how='left',
                                on=['model', 'variable', 'experiment', 'tolerance', 'draw', 'archive']).copy()
    del (gen_stats_jump)

    compare = gen_stats.merge(orig_stats, how='left', on=['model', 'variable', 'experiment']).copy()
    del (gen_stats)
    del (orig_stats)

    compare['E1_tgav'] = abs(compare.mean_orig_tgav - compare.mean_gen_tgav) / compare.sd_orig_tgav
    compare['E2_tgav'] = compare.sd_gen_tgav / compare.sd_orig_tgav

    compare['E1_jump'] = abs(compare.mean_orig_jump - compare.mean_gen_jump) / compare.sd_orig_jump
    compare['E2_jump'] = compare.sd_gen_jump / compare.sd_orig_jump

    compare = compare[['model', 'variable', 'experiment', 'tolerance', 'draw', 'archive',
                       'E1_tgav', 'E2_tgav', 'E1_jump', 'E2_jump']].copy()

    four_values = []
    for name, group in compare.groupby(['model', 'variable', 'experiment', 'tolerance', 'draw', 'archive']):
        ds = group.copy()
        ds['max_metric'] = np.max(
            [ds.E1_tgav.values, abs(1 - ds.E2_tgav.values), ds.E1_jump.values, abs(1 - ds.E2_jump.values)])
        four_values.append(ds)
        del (ds)
    four_values = pd.concat(four_values).reset_index(drop=True).copy()
    del (compare)

    return(four_values)


# #############################################################################
# The experiment
# #############################################################################

# for each of the esms in the experiment, subset to what we want
# to work with and run the experiment.
for esm in esms:
    print(esm)

    # subset the archive and the targets to this ESM
    archive_w_all = full_archive_data[(full_archive_data['model'] == esm)].copy()

    archive_wo245 = full_archive_data[(full_archive_data['model'] == esm) &
                                      (full_archive_data['experiment'] != 'ssp245')].copy()

    archive_wo370 = full_archive_data[(full_archive_data['model'] == esm) &
                                      (full_archive_data['experiment'] != 'ssp370')].copy()


    target_245 = full_target_data[(full_target_data['model'] == esm) &
                                  (full_target_data['experiment'] == 'ssp245')].copy()

    target_370 = full_target_data[(full_target_data['model'] == esm) &
                                  (full_target_data['experiment'] == 'ssp370')].copy()


    # Some models (HadGEM3-GC31-LL for example), have realizations that stop
    # in 2014. Our recipe permutation only works when every target realization
    # has the same time series (and the same discretization of the time series).
    # So we generally drop those realizations from the target dataframe.
    if not target_245.empty:
        grped_245 = target_245.groupby(['experiment', 'variable', 'ensemble', 'model'])
        for name, group in grped_245:
            df1 = group.copy()
            # if it isn't a complete time series (defined as going to 2099 or 2100),
            # remove it from the target data frame:
            if max(df1.end_yr) < 2099:
                target_245 = target_245.loc[(target_245['ensemble'] != df1.ensemble.unique()[0])].copy().reset_index(drop=True)
            del (df1)
        del (grped_245)

    if not target_370.empty:
        grped_370 = target_370.groupby(['experiment', 'variable', 'ensemble', 'model'])
        for name, group in grped_370:
            df1 = group.copy()
            # if it isn't a complete time series (defined as going to 2099 or 2100),
            # remove it from the target data frame:
            if max(df1.end_yr) < 2099:
                target_370 = target_370.loc[(target_370['ensemble'] != df1.ensemble.unique()[0])].copy().reset_index(
                    drop=True)
            del(df1)
        del(grped_370)

    target_245 = target_245.reset_index(drop=True).copy()
    target_370 = target_370.reset_index(drop=True).copy()


    # Load in the raw, unsmoothed/unchunked target data.
    full_rawtarget_path = pkg_resources.resource_filename('stitches', ('data/tas-data/' + esm + '_tas.csv'))
    full_rawtarget_data = pd.read_csv(full_rawtarget_path)

    orig_245 = full_rawtarget_data[(full_rawtarget_data['experiment'] == 'ssp245')].copy()
    keys = ['experiment', 'ensemble', 'model']
    i1 = orig_245.set_index(keys).index
    i2 = target_245.set_index(keys).index
    orig_245 = orig_245[i1.isin(i2)].copy()
    del (i1)
    del (i2)
    del (keys)
    orig_245 = orig_245.reset_index(drop=True).copy()


    orig_370 = full_rawtarget_data[(full_rawtarget_data['experiment'] == 'ssp370')].copy()
    keys = ['experiment', 'ensemble', 'model']
    i1 = orig_370.set_index(keys).index
    i2 = target_370.set_index(keys).index
    orig_370 = orig_370[i1.isin(i2)].copy()
    orig_370 = orig_370.reset_index(drop=True).copy()
    del (i1)
    del (i2)
    del (keys)
    del(full_rawtarget_data)
    del(full_rawtarget_path)


    # loop over tolerances:
    for tolerance in tolerances:
        print(tolerance)

        # Use the match_neighborhood function to generate all of the matches between the target and
        # archive data points.
        if not target_245.empty:
            match_245_w = stitches.match_neighborhood(target_245, archive_w_all, tol=tolerance)

            match_245_wo = stitches.match_neighborhood(target_245, archive_wo245, tol=tolerance)

        else:
            print('No target ssp245 data for ' + esm + '. Analysis will be skipped')

        if not target_370.empty:
            match_370_w = stitches.match_neighborhood(target_370, archive_w_all, tol=tolerance)

            match_370_wo = stitches.match_neighborhood(target_370, archive_wo370, tol=tolerance)

        else:
            print('No target ssp370 data for ' + esm + '. Analysis will be skipped')

        # loop over draws for the tolerance
        for draw in range(0, Ndraws):
            # use the permute_stitching_recipes function to do the draws.
            # Make N_matches very large just so the full collapse free ensemble is generated
            if not target_245.empty:
                unformatted_recipe_245_w = stitches.permute_stitching_recipes(N_matches=10000,
                                                                              matched_data=match_245_w,
                                                                              archive=archive_w_all, testing=False)
                new_ids = ('tol' + str(tolerance) + '~draw' + str(draw) + '~archive_w~' +
                           unformatted_recipe_245_w['stitching_id'].astype(str)).copy()
                unformatted_recipe_245_w = unformatted_recipe_245_w.drop(columns=['stitching_id']).copy()
                unformatted_recipe_245_w['stitching_id'] = new_ids
                del (new_ids)


                unformatted_recipe_245_wo = stitches.permute_stitching_recipes(N_matches=10000,
                                                                               matched_data=match_245_wo,
                                                                               archive=archive_wo245, testing=False)
                new_ids = ('tol' + str(tolerance) + '~draw' + str(draw) + '~archive_wo~' +
                           unformatted_recipe_245_wo['stitching_id'].astype(str)).copy()
                unformatted_recipe_245_wo = unformatted_recipe_245_wo.drop(columns=['stitching_id']).copy()
                unformatted_recipe_245_wo['stitching_id'] = new_ids
                del (new_ids)


            else:
                print('No target ssp245 data for ' + esm + '. Matching skipped')

            if not target_370.empty:
                unformatted_recipe_370_w = stitches.permute_stitching_recipes(N_matches=10000,
                                                                              matched_data=match_370_w,
                                                                              archive=archive_w_all, testing=False)
                new_ids = ('tol' + str(tolerance) + '~draw' + str(draw) + '~archive_w~' +
                           unformatted_recipe_370_w['stitching_id'].astype(str)).copy()
                unformatted_recipe_370_w = unformatted_recipe_370_w.drop(columns=['stitching_id']).copy()
                unformatted_recipe_370_w['stitching_id'] = new_ids
                del (new_ids)


                unformatted_recipe_370_wo = stitches.permute_stitching_recipes(N_matches=10000,
                                                                               matched_data=match_370_wo,
                                                                               archive=archive_wo370, testing=False)
                new_ids  = ( 'tol' + str(tolerance) + '~draw' + str(draw) + '~archive_wo~' +
                             unformatted_recipe_370_wo['stitching_id'].astype(str) ).copy()
                unformatted_recipe_370_wo = unformatted_recipe_370_wo.drop(columns = ['stitching_id']).copy()
                unformatted_recipe_370_wo['stitching_id'] = new_ids
                del(new_ids)


            else:
                print('No target ssp370 data for ' + esm + '. Matching skipped')

            # Clean up the recipe so that it can be used to generate the gridded data products.
            if not target_245.empty:
                recipe_245_w = stitches.generate_gridded_recipe(unformatted_recipe_245_w)
                recipe_245_w.columns = ['target_start_yr', 'target_end_yr', 'archive_experiment', 'archive_variable',
                                        'archive_model', 'archive_ensemble', 'stitching_id', 'archive_start_yr',
                                        'archive_end_yr', 'tas_file']
                recipe_245_w['tolerance'] = tolerance
                recipe_245_w['draw'] = draw
                recipe_245_w['archive'] = 'w_target'
                recipe_245_w.to_csv((OUTPUT_DIR + '/' + esm + '/experiment_CMIP6/' +
                                     'gridded_recipes_' + esm + '_target245_tol' + str(tolerance) +
                                     '_draw' + str(draw) + '_archive_w_target.csv'), index=False)
                del(unformatted_recipe_245_w)

                recipe_245_wo = stitches.generate_gridded_recipe(unformatted_recipe_245_wo)
                recipe_245_wo.columns = ['target_start_yr', 'target_end_yr', 'archive_experiment', 'archive_variable',
                                         'archive_model', 'archive_ensemble', 'stitching_id', 'archive_start_yr',
                                         'archive_end_yr', 'tas_file']
                recipe_245_wo['tolerance'] = tolerance
                recipe_245_wo['draw'] = draw
                recipe_245_wo['archive'] = 'wo_target'
                recipe_245_wo.to_csv((OUTPUT_DIR + '/' + esm + '/experiment_CMIP6/' +
                                     'gridded_recipes_' + esm + '_target245_tol' + str(tolerance) +
                                     '_draw' + str(draw) + '_archive_wo_target.csv'), index=False)
                del (unformatted_recipe_245_wo)


            else:
                print('No target ssp245 data for ' + esm + '. Recipe formatting skipped')

            if not target_370.empty:
                recipe_370_w = stitches.generate_gridded_recipe(unformatted_recipe_370_w)
                recipe_370_w.columns = ['target_start_yr', 'target_end_yr', 'archive_experiment', 'archive_variable',
                                        'archive_model', 'archive_ensemble', 'stitching_id', 'archive_start_yr',
                                        'archive_end_yr', 'tas_file']
                recipe_370_w['tolerance'] = tolerance
                recipe_370_w['draw'] = draw
                recipe_370_w['archive'] = 'w_target'
                recipe_370_w.to_csv((OUTPUT_DIR + '/' + esm + '/experiment_CMIP6/' +
                                     'gridded_recipes_' + esm + '_target370_tol' + str(tolerance) +
                                     '_draw' + str(draw) + '_archive_w_target.csv'), index=False)
                del (unformatted_recipe_370_w)


                recipe_370_wo = stitches.generate_gridded_recipe(unformatted_recipe_370_wo)
                recipe_370_wo.columns = ['target_start_yr', 'target_end_yr', 'archive_experiment', 'archive_variable',
                                         'archive_model', 'archive_ensemble', 'stitching_id', 'archive_start_yr',
                                         'archive_end_yr', 'tas_file']
                recipe_370_wo['tolerance'] = tolerance
                recipe_370_wo['draw'] = draw
                recipe_370_wo['archive'] = 'wo_target'
                recipe_370_wo.to_csv((OUTPUT_DIR + '/' + esm + '/experiment_CMIP6/' +
                                      'gridded_recipes_' + esm + '_target370_tol' + str(tolerance) +
                                      '_draw' + str(draw) + '_archive_wo_target.csv'), index=False)
                del (unformatted_recipe_370_wo)

            else:
                print('No target ssp370 data for ' + esm + '. Recipe formatting skipped')

            # stitch the GSAT values and save as csv
            try:
                if not target_245.empty:
                    gsat_245_w = stitches.gmat_stitching(recipe_245_w)
                    gsat_245_w['tolerance'] = tolerance
                    gsat_245_w['draw'] = draw
                    gsat_245_w['archive'] = 'w_target'
                    del(recipe_245_w)

                    for id in gsat_245_w.stitching_id.unique():
                        ds = gsat_245_w[gsat_245_w['stitching_id'] == id].copy()
                        fname = (OUTPUT_DIR + '/' + esm + '/experiment_CMIP6/' +
                                 'stitched_' + esm + '_GSAT_' + id + '.csv')
                        ds.to_csv(fname, index=False)
                        del(ds)
                    del(gsat_245_w)

                    gsat_245_wo = stitches.gmat_stitching(recipe_245_wo)
                    gsat_245_wo['tolerance'] = tolerance
                    gsat_245_wo['draw'] = draw
                    gsat_245_wo['archive'] = 'wo_target'
                    del (recipe_245_wo)

                    for id in gsat_245_wo.stitching_id.unique():
                        ds = gsat_245_wo[gsat_245_wo['stitching_id'] == id].copy()
                        fname = (OUTPUT_DIR + '/' + esm + '/experiment_CMIP6/' +
                                 'stitched_' + esm + '_GSAT_' + id + '.csv')
                        ds.to_csv(fname, index=False)
                        del (ds)
                    del(gsat_245_wo)

                else:
                    print('No target ssp245 data for ' + esm + '. GSAT stitching skipped')


                if not target_370.empty:
                    gsat_370_w = stitches.gmat_stitching(recipe_370_w)
                    gsat_370_w['tolerance'] = tolerance
                    gsat_370_w['draw'] = draw
                    gsat_370_w['archive'] = 'w_target'
                    del(recipe_370_w)

                    for id in gsat_370_w.stitching_id.unique():
                        ds = gsat_370_w[gsat_370_w['stitching_id'] == id].copy()
                        fname = (OUTPUT_DIR + '/' + esm + '/experiment_CMIP6/' +
                                 'stitched_' + esm + '_GSAT_' + id + '.csv')
                        ds.to_csv(fname, index=False)
                        del(ds)
                    del(gsat_370_w)


                    gsat_370_wo = stitches.gmat_stitching(recipe_370_wo)
                    gsat_370_wo['tolerance'] = tolerance
                    gsat_370_wo['draw'] = draw
                    gsat_370_wo['archive'] = 'wo_target'
                    del(recipe_370_wo)

                    for id in gsat_370_wo.stitching_id.unique():
                        ds = gsat_370_wo[gsat_370_wo['stitching_id'] == id].copy()
                        fname = (OUTPUT_DIR + '/' + esm + '/experiment_CMIP6/' +
                                 'stitched_' + esm + '_GSAT_' + id + '.csv')
                        ds.to_csv(fname, index=False)
                        del (ds)
                    del(gsat_370_wo)

                else:
                    print('No target ssp370 data for ' + esm + '. GSAT stitching skipped')

            # end try
            except:
                print(("Some issue stitching GMAT for " + esm + ". Skipping and moving on"))
        # end for loop over draws
    # end for loop over tolerances

    #########################################################
    # Now we've generated all the GSAT files we're going to for each target.
    # It's time to compare to the raw ensemble statistics for both Tgav and jumps.

    if (((not orig_245.empty) & (not target_245.empty)) |
            ((not orig_370.empty) & (not target_370.empty))):

        # form the raw ensemble data frame
        if orig_245.empty:
            orig = orig_370.reset_index(drop=True).copy()
        elif orig_370.empty:
            orig = orig_245.reset_index(drop=True).copy()
        else:
            orig = orig_245.append(orig_370).reset_index(drop=True).copy()

        # Read in all generated GSAT files and format so error metrics can
        # be calculated.
        gen = []
        entries = Path((OUTPUT_DIR + '/' + esm + '/experiment_CMIP6/'))
        for entry in entries.iterdir():
            if (('stitched' in entry.name) & ('GSAT' in entry.name)):
                data = pd.read_csv((OUTPUT_DIR + '/' + esm + '/experiment_CMIP6/') + entry.name)
                data['model'] = esm

                if ('ssp245' in entry.name):
                    data['experiment'] = 'ssp245'

                if ('ssp370' in entry.name):
                    data['experiment'] = 'ssp370'

                gen.append(data)
                del (data)
        gen = pd.concat(gen).reset_index(drop=True).copy()
        gen = gen.rename(columns={"stitching_id": "ensemble"}).copy()

        compared_data = four_errors(gen_data=gen, orig_data=orig)
        compared_data.to_csv((OUTPUT_DIR + '/' + esm + '/experiment_CMIP6/all_metrics.csv'), index=False)

        # average over draws
        metrics_avg_over_draws = []
        for name, group in compared_data.groupby(['model', 'variable', 'experiment', 'tolerance', 'archive']):
            ds = group.copy()
            ds1 = ds[['model', 'variable', 'experiment', 'tolerance', 'archive']].drop_duplicates().copy()
            ds1['mean_E1_tgav'] = np.mean(ds.E1_tgav.values)
            ds1['mean_E2_tgav'] = np.mean(ds.E2_tgav.values)
            ds1['mean_E1_jump'] = np.mean(ds.E1_jump.values)
            ds1['mean_E2_jump'] = np.mean(ds.E2_jump.values)
            ds1['max_metric'] = np.max([ds1.mean_E1_tgav.values,
                                        abs(1 - ds1.mean_E2_tgav.values),
                                        ds1.mean_E1_jump.values,
                                        abs(1 - ds1.mean_E2_jump.values)])
            metrics_avg_over_draws.append(ds1)
            del (ds)
            del (ds1)
        metrics_avg_over_draws = pd.concat(metrics_avg_over_draws).reset_index(drop=True).copy()

        # filter to the largest tolerance that keeps max_metric<error_threshold
        # for each model, variable, experiment, archive.
        max_tol = []
        for name, group in metrics_avg_over_draws.groupby(['model', 'variable', 'experiment', 'archive']):
            ds = group.copy()
            ds = ds[ds["max_metric"] < error_threshold].copy()
            ds = ds[ds['tolerance'] == ds.tolerance.max()].copy()
            ds = ds.rename(columns={"tolerance": "max_tol"}).copy()
            ds = ds[['model', 'variable', 'experiment', 'archive', 'max_tol']].copy()
            max_tol.append(ds)
            del (ds)
        max_tol = pd.concat(max_tol).reset_index(drop=True).copy()
        max_tol.to_csv((OUTPUT_DIR + '/' + esm + '/experiment_CMIP6/max_tol_by_ESM_SSP.csv'), index=False)


    if not max_tol.empty:
        # now that we have max tolerances, we will use those to do a final,
        # reproducible draw and construction of gridded data
        for name, group in max_tol.groupby(['model', 'variable', 'experiment', 'archive']):
            tolerance = group.max_tol.unique()[0]
            arch_id = group.archive.unique()[0]
            targ_id = group.experiment.unique()[0]

            if targ_id == 'ssp245':
                target = target_245.copy()
                if group.archive.unique == 'wo_target':
                    archive = archive_wo245.copy()
                else:
                    archive = archive_w_all.copy()

            if targ_id == 'ssp370':
                target = target_370.copy()
                if group.archive.unique == 'wo_target':
                    archive = archive_wo370.copy()
                else:
                    archive = archive_w_all.copy()

            match_df = stitches.match_neighborhood(target, archive,
                                                   tol=tolerance)

            unformatted_recipe = stitches.permute_stitching_recipes(N_matches=10000,
                                                                    matched_data=match_df,
                                                                    archive=archive, testing=True)
            new_ids = ('MAXTOLRUN ~tol' + str(tolerance) + '~archive_' + arch_id + '~' +
                       unformatted_recipe['stitching_id'].astype(str)).copy()
            unformatted_recipe = unformatted_recipe.drop(columns=['stitching_id']).copy()
            unformatted_recipe['stitching_id'] = new_ids
            del (new_ids)

            recipe = stitches.generate_gridded_recipe(unformatted_recipe)
            recipe.columns = ['target_start_yr', 'target_end_yr', 'archive_experiment', 'archive_variable',
                              'archive_model', 'archive_ensemble', 'stitching_id', 'archive_start_yr',
                              'archive_end_yr', 'tas_file']
            recipe.to_csv((OUTPUT_DIR + '/' + esm + '/experiment_CMIP6/' +
                               'MAXTOLRUN_gridded_recipes_' + esm + '_target'+
                           targ_id +'_archive_' + arch_id +'.csv'), index=False)


            gsat = stitches.gmat_stitching(recipe)
            gsat['tolerance'] = tolerance
            gsat['archive'] = arch_id
            for id in gsat.stitching_id.unique():
                ds = gsat[gsat['stitching_id'] == id].copy()
                fname = (OUTPUT_DIR + '/' + esm + '/experiment_CMIP6/' +
                         'MAXTOLRUN_stitched_' + esm + '_GSAT_' + id + '.csv')
                ds.to_csv(fname, index=False)


                for single_id in recipe['stitching_id'].unique():
                    single_rp = recipe.loc[recipe['stitching_id'] == single_id].copy()
                    outputs = stitches.gridded_stitching((OUTPUT_DIR + '/' + esm + '/experiment_CMIP6'),
                                                         single_rp)
                    del (single_rp)
                    del(outputs)




# end for loop over ESMs
