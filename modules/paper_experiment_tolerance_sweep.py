# Perform tolerance sweep  tolerance by ESM-SSP and  well-characterized errors
# for an audience seeking to emulate from the full CMIP6 archive and for the
# scenarioMIP approach,  for all ESMs

# For each ESM, loop over various tolerances and generate Ndraws = 500
# GSAT trajectories. Archives with and without each target to characterize
# error. (Reproducible mode off so draws are different). And the ScenMIP.
# Compare to the target ensemble via 4 metrics (E1, E2 on both Tgavs and
# jumps) and record errors for each draw and tolerance.

## TODO: comment out saving the GSATs

## TODO functionalize at least some part of the analysis or at least use a for-loop
## over the different SSP targets so that the code isn't so long and repetitive
# making a table of avail runs X planned archives and for looping over that would
# trim things down (see approach for max tol runs). And rewrite the tolerance
# iteration to be a while loop, comparing current to prev instead of calculating
# and saving it all? Update writes and reads to be subdir so things are tidier

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

# pd.set_option('display.max_columns', None)

# OUTPUT_DIR = pkg_resources.resource_filename('stitches', 'data/created_data')
OUTPUT_DIR = '/pic/projects/GCAM/stitches_pic/paper1_outputs'

# #############################################################################
# Experiment  setup
# #############################################################################
# experiment parameters
tolerances = np.round(np.arange(0.01, 0.225, 0.005), 3)
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

esms = pangeo_good_ensembles.model.unique().copy()


# #############################################################################
# Load full archive and target data
# #############################################################################

# Load the full archive of all staggered windows, which we will be matching on
full_archive_path = pkg_resources.resource_filename('stitches', 'data/matching_archive.csv')
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
def prep_target_data(target_df):
    if not target_df.empty:
        grped = target_df.groupby(['experiment', 'variable', 'ensemble', 'model'])
        for name, group in grped:
            df1 = group.copy()
            # if it isn't a complete time series (defined as going to 2099 or 2100),
            # remove it from the target data frame:
            if max(df1.end_yr) < 2099:
                target_df = target_df.loc[(target_df['ensemble'] != df1.ensemble.unique()[0])].copy().reset_index(
                    drop=True)
            del (df1)
        del (grped)

        target_df = target_df.reset_index(drop=True).copy()
        return(target_df)


def get_orig_data(target_df):
    if not target_df.empty:
        esm_name = target_df.model.unique()[0]
        scn_name = target_df.experiment.unique()[0]

        full_rawtarget_path = pkg_resources.resource_filename('stitches', ('data/tas-data/' + esm_name + '_tas.csv'))
        full_rawtarget_data = pd.read_csv(full_rawtarget_path)

        orig_data = full_rawtarget_data[(full_rawtarget_data['experiment'] == scn_name)].copy()
        keys = ['experiment', 'ensemble', 'model']
        i1 = orig_data.set_index(keys).index
        i2 = target_df.set_index(keys).index
        orig_data = orig_data[i1.isin(i2)].copy()
        del (i1)
        del (i2)
        del (keys)
        del (full_rawtarget_data)
        del (full_rawtarget_path)

        orig_data = orig_data.reset_index(drop=True).copy()
        return (orig_data)


def match_draw_stitchTgav(target_df, archive_df, toler, num_draws, TGAV_OUTPUT_DIR, reproducible):

    esm_name = archive_df.model.unique()[0]

    if not target_df.empty:
        # Use the match_neighborhood function to generate all of the matches between the target and
        # archive data points.
        match_df = stitches.match_neighborhood(target_df, archive_df, tol=toler)
        scn_name = target_df.experiment.unique()[0]

        if ((not ('ssp245' in archive_df.experiment.unique())) & (not ('ssp370' in archive_df.experiment.unique()))):
            archive_id = 'scenarioMIP'
        elif scn_name in archive_df.experiment.unique():
            archive_id = 'w_target'
        else:
            archive_id = 'wo_target'


        for draw in range(0, num_draws):
            # Do the random draw of recipes
            if reproducible:
                unformatted_recipe = stitches.permute_stitching_recipes(N_matches=10000,
                                                                        matched_data=match_df,
                                                                        archive=archive_df,
                                                                        testing=True)
            else:
                unformatted_recipe = stitches.permute_stitching_recipes(N_matches=10000,
                                                                        matched_data=match_df,
                                                                        archive=archive_df,
                                                                        testing=False)


            new_ids = ('tol' + str(toler) + '~draw' + str(draw) + '~' + archive_id + '~'+
                       unformatted_recipe['stitching_id'].astype(str)).copy()
            unformatted_recipe = unformatted_recipe.drop(columns=['stitching_id']).copy()
            unformatted_recipe['stitching_id'] = new_ids
            del (new_ids)

            # format the recipe
            recipe = stitches.generate_gridded_recipe(unformatted_recipe)
            recipe.columns = ['target_start_yr', 'target_end_yr', 'archive_experiment', 'archive_variable',
                              'archive_model', 'archive_ensemble', 'stitching_id', 'archive_start_yr',
                              'archive_end_yr', 'tas_file']
            recipe['tolerance'] = toler
            recipe['draw'] = draw
            recipe['archive'] = archive_id
            recipe.to_csv((OUTPUT_DIR + '/' + esm_name + '/experiment_CMIP6/' +
                           'gridded_recipes_' + esm_name + '_target_' + scn_name +
                           '_tol' + str(toler) +
                           '_draw' + str(draw) +
                           '_archive_' + archive_id + '.csv'), index=False)
            del (unformatted_recipe)

            # stitch the GSAT values and save as csv
            try:

                gsat = stitches.gmat_stitching(recipe)
                gsat['tolerance'] = toler
                gsat['draw'] = draw
                gsat['archive'] = archive_id
                for id in gsat.stitching_id.unique():
                    ds = gsat[gsat['stitching_id'] == id].copy()
                    fname = (TGAV_OUTPUT_DIR +
                             'stitched_' + esm_name + '_GSAT_' + id + '.csv')
                    ds.to_csv(fname, index=False)
                    del (ds)

                del (gsat)

            except:
                print(("Some issue stitching GMAT for " + esm_name + ". Skipping and moving on"))

    else:
        recipe = []
        print('Some missing target data for ' + esm_name + '. Analysis will be skipped')

    return(recipe)


def get_jumps(tgav_df):
    tgav_jump = []
    for name, group in tgav_df.groupby(['variable', 'experiment', 'ensemble', 'model']):
        ds = group.copy()
        ds['jump'] = ds.value.diff().copy()
        ds = ds.dropna().copy()
        tgav_jump.append(ds)
        del (ds)
    tgav_jump = pd.concat(tgav_jump)
    tgav_jump = tgav_jump.drop(columns=['value']).copy()
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
        ds1['mean_orig_jump'] = np.mean(ds.jump.values)
        ds1['sd_orig_jump'] = np.std(ds.jump.values)
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
        ds1['mean_gen_jump'] = np.mean(ds.jump.values)
        ds1['sd_gen_jump'] = np.std(ds.jump.values)
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


def match_draw_stitch_evalTgav(target_df, archive_df, toler, num_draws, ERR_OUTPUT_DIR, reproducible):

    esm_name = archive_df.model.unique()[0]

    if not target_df.empty:
        # Use the match_neighborhood function to generate all of the matches between the target and
        # archive data points.
        match_df = stitches.match_neighborhood(target_df, archive_df, tol=toler)
        scn_name = target_df.experiment.unique()[0]

        # get corresponding original data to the target
        orig_df = get_orig_data(target_df).copy()

        if  ((not ('ssp245' in archive_df.experiment.unique())) & (not ('ssp370' in archive_df.experiment.unique()))):
            archive_id = 'scenarioMIP'
        elif scn_name in archive_df.experiment.unique():
            archive_id = 'w_target'
        else:
            archive_id = 'wo_target'


        for draw in range(0, num_draws):
            # Do the random draw of recipes
            if reproducible:
                unformatted_recipe = stitches.permute_stitching_recipes(N_matches=10000,
                                                                        matched_data=match_df,
                                                                        archive=archive_df,
                                                                        testing=True)
            else:
                unformatted_recipe = stitches.permute_stitching_recipes(N_matches=10000,
                                                                        matched_data=match_df,
                                                                        archive=archive_df,
                                                                        testing=False)


            new_ids = ('tol' + str(toler) + '~draw' + str(draw) + '~' + archive_id + '~'+
                       unformatted_recipe['stitching_id'].astype(str)).copy()
            unformatted_recipe = unformatted_recipe.drop(columns=['stitching_id']).copy()
            unformatted_recipe['stitching_id'] = new_ids
            del (new_ids)

            # format the recipe
            recipe = stitches.generate_gridded_recipe(unformatted_recipe)
            recipe.columns = ['target_start_yr', 'target_end_yr', 'archive_experiment', 'archive_variable',
                              'archive_model', 'archive_ensemble', 'stitching_id', 'archive_start_yr',
                              'archive_end_yr', 'tas_file']
            recipe['tolerance'] = toler
            recipe['draw'] = draw
            recipe['archive'] = archive_id
            # recipe.to_csv((OUTPUT_DIR + '/' + esm_name + '/experiment_CMIP6/' +
            #                'gridded_recipes_' + esm_name + '_target_' + scn_name +
            #                '_tol' + str(toler) +
            #                '_draw' + str(draw) +
            #                '_archive_' + archive_id + '.csv'), index=False)
            del (unformatted_recipe)

            # stitch the GSAT values and save as csv
            gsat = stitches.gmat_stitching(recipe)
            gsat['tolerance'] = toler
            gsat['draw'] = draw
            gsat['archive'] = archive_id
            gsat['experiment'] = scn_name
            gsat['model'] = esm_name
            for id in gsat.stitching_id.unique():
                ds = gsat[gsat['stitching_id'] == id].copy()

                # errors vs orig data
                ds = ds.rename(columns={"stitching_id": "ensemble"}).copy()
                compared_ds = four_errors(gen_data=ds, orig_data=orig_df)
                compared_ds['stitching_id'] = id

                fname = (ERR_OUTPUT_DIR + 'all_metrics_' + esm_name + '_' + id + '.csv')
                print(fname)
                compared_ds.to_csv(fname, index=False)

                del (ds)
                del (compared_ds)

            del (gsat)


    else:
        recipe = []
        print('Some missing target data for ' + esm_name + '. Analysis will be skipped')

    return(recipe)



# #############################################################################
# The experiment
# #############################################################################

# for each of the esms in the experiment, subset to what we want
# to work with and run the experiment.
for esm in esms[0:1]:
    print(esm)

    # subset the archive and the targets to this ESM
    archive_w_all = full_archive_data[(full_archive_data['model'] == esm)].copy()

    archive_wo245 = full_archive_data[(full_archive_data['model'] == esm) &
                                      (full_archive_data['experiment'] != 'ssp245')].copy()

    archive_wo370 = full_archive_data[(full_archive_data['model'] == esm) &
                                      (full_archive_data['experiment'] != 'ssp370')].copy()

    archive_scenMIP = full_archive_data[(full_archive_data['model'] == esm) &
                                        ((full_archive_data['experiment'] == 'ssp126') |
                                         (full_archive_data['experiment'] == 'ssp585'))].copy()


    target_245 = full_target_data[(full_target_data['model'] == esm) &
                                  (full_target_data['experiment'] == 'ssp245')].copy()

    target_370 = full_target_data[(full_target_data['model'] == esm) &
                                  (full_target_data['experiment'] == 'ssp370')].copy()


    # Clean up target data and pull corresponding original/raw data
    if not target_245.empty:
        # clean up
        target_245 = prep_target_data(target_245).copy()


    if not target_370.empty:
        # clean up
        target_370 = prep_target_data(target_370).copy()


    # loop over tolerances:
    for tolerance in tolerances[0:1]:

        rp_245_w = match_draw_stitch_evalTgav(target_245, archive_w_all,
                                              toler=tolerance, num_draws=Ndraws,
                                              ERR_OUTPUT_DIR=(OUTPUT_DIR +'/tolerance_sweeps/alL_draws/'),
                                              reproducible=False)
        rp_245_wo = match_draw_stitch_evalTgav(target_245, archive_wo245,
                                               toler=tolerance, num_draws=Ndraws,
                                               ERR_OUTPUT_DIR=(OUTPUT_DIR + '/tolerance_sweeps/alL_draws/'),
                                               reproducible=False)
        rp_370_w = match_draw_stitch_evalTgav(target_370, archive_w_all,
                                              toler=tolerance, num_draws=Ndraws,
                                              ERR_OUTPUT_DIR=(OUTPUT_DIR +'/tolerance_sweeps/alL_draws/'),
                                              reproducible=False)
        rp_370_wo = match_draw_stitch_evalTgav(target_370, archive_wo370,
                                               toler=tolerance, num_draws=Ndraws,
                                               ERR_OUTPUT_DIR=(OUTPUT_DIR +'/tolerance_sweeps/alL_draws/'),
                                               reproducible=False)

        rp_245_scenMIP = match_draw_stitch_evalTgav(target_245, archive_scenMIP,
                                                    toler=tolerance, num_draws=Ndraws,
                                                    ERR_OUTPUT_DIR=(OUTPUT_DIR +'/tolerance_sweeps/alL_draws/'),
                                                    reproducible=False)

        rp_370_scenMIP = match_draw_stitch_evalTgav(target_370, archive_scenMIP,
                                                    toler=tolerance, num_draws=Ndraws,
                                                    ERR_OUTPUT_DIR=(OUTPUT_DIR +'/tolerance_sweeps/alL_draws/'),
                                                    reproducible=False)


    #########################################################

        # Read in all generated GSAT files and format so error metrics can
        # be calculated.
        compared_data = []
        entries = Path((OUTPUT_DIR + '/tolerance_sweeps/all_draws/'))
        for entry in entries.iterdir():
            if (('all_metrics' in entry.name) & (esm in entry.name)):
                data = pd.read_csv((OUTPUT_DIR  + '/tolerance_sweeps/all_draws/') + entry.name)
                compared_data.append(data)
                del (data)

        if len(compared_data) > 0:
            compared_data = pd.concat(compared_data).reset_index(drop=True).copy()

            # average over draws
            aggregate_metrics = []
            for name, group in compared_data.groupby(['model', 'variable', 'experiment', 'tolerance', 'archive']):
                ds = group.copy()
                ds1 = ds[['model', 'variable', 'experiment', 'tolerance', 'archive']].drop_duplicates().copy()
                ds1['aggregate_E1_tgav'] = np.mean(ds.E1_tgav.values)
                ds1['aggregate_E2_tgav'] = np.mean(ds.E2_tgav.values)
                ds1['aggregate_E1_jump'] = np.mean(ds.E1_jump.values)
                ds1['aggregate_E2_jump'] = np.mean(ds.E2_jump.values)
                ds1['max_metric'] = np.max([ds1.aggregate_E1_tgav.values,
                                            abs(1 - ds1.aggregate_E2_tgav.values),
                                            ds1.aggregate_E1_jump.values,
                                            abs(1 - ds1.aggregate_E2_jump.values)])
                aggregate_metrics.append(ds1)
                del (ds)
                del (ds1)
            aggregate_metrics = pd.concat(aggregate_metrics).reset_index(drop=True).copy()
            aggregate_metrics.to_csv((OUTPUT_DIR + '/tolerance_sweeps/aggregate_metrics_' + esm + '.csv'), index=False)

            # filter to the largest tolerance that keeps max_metric<error_threshold
            # for each model, variable, experiment, archive.
            max_tol = []
            for name, group in aggregate_metrics.groupby(['model', 'variable', 'experiment', 'archive']):
                ds = group.copy()
                ds = ds[ds["max_metric"] < error_threshold].copy()
                ds = ds[ds['tolerance'] == ds.tolerance.max()].copy()
                ds = ds.rename(columns={"tolerance": "max_tol"}).copy()
                ds = ds[['model', 'variable', 'experiment', 'archive', 'max_tol']].copy()
                max_tol.append(ds)
                del (ds)
            max_tol = pd.concat(max_tol).reset_index(drop=True).copy()
            max_tol.to_csv((OUTPUT_DIR + '/tolerance_sweeps/max_tol_0p1_' + esm + '.csv'), index=False)

# end for loop over ESMs
