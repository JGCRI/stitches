# Using SSP126 and SSP585 as bracketing scenarios,
# emulate  SSP245 and SSP370 as target scenarios.
# For every ESM with monthly tas available.
# Using tol=0.08degC (min maxTol found in offline experiments), to be
# safe since we haven't found maxTol for each ESM yet.

# would be better to functionalize this script with ESM, tol and Ndraws as arguments
# and then have the .sh just call the function and dispatch to diff nodes for each run I guess.

# #############################################################################
# General setup
# #############################################################################
# Import packages

import pandas as pd
import stitches as stitches
import pkg_resources

# pd.set_option('display.max_columns', None)

# OUTPUT_DIR = pkg_resources.resource_filename('stitches', 'data/created_data')
OUTPUT_DIR = '/pic/projects/GCAM/stitches_pic/new_scenarioMIP_experiments'

# #############################################################################
# Experiment  setup
# #############################################################################
# experiment parameters
tolerance = 0.075
Ndraws = 1

# #############################################################################
# Helper functions
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

# #############################################################################
# The experiment
# #############################################################################

esms = ['ACCESS-CM2', 'ACCESS-ESM1-5', 'BCC-CSM2-MR','CAMS-CSM1-0', 'CAS-ESM2-0',
        'CMCC-CM2-SR5', 'CMCC-ESM2', 'CanESM5','FGOALS-g3', 'FIO-ESM-2-0',
        'GISS-E2-1-G', 'HadGEM3-GC31-LL', 'MCM-UA-1-0', 'MIROC-ES2L', 'MIROC6',
        'MPI-ESM1-2-HR', 'MPI-ESM1-2-LR', 'MRI-ESM2-0', 'NESM3', 'NorESM2-LM', 'NorESM2-MM',
        'TaiESM1', 'UKESM1-0-LL']


# Load the full archive of all staggered windows, which we will be matching on
full_archive_path = pkg_resources.resource_filename('stitches', 'data/matching_archive.csv')
full_archive_data = pd.read_csv(full_archive_path)

# Load the original archive without staggered windows, which we will draw
# the target trajectories from for matching
full_target_path = pkg_resources.resource_filename('stitches', 'data/matching_archive.csv')
full_target_data = pd.read_csv(full_target_path)

# for each of the esms in the experiment, subset to what we want
# to work with and run the experiment.
for esm in esms:
    print(esm)

    try:
        # subset the archive and the targets to this ESM
        archive_data = full_archive_data[(full_archive_data['model'] == esm) &
                                         ((full_archive_data['experiment'] == 'ssp126') |
                                          (full_archive_data['experiment'] == 'ssp585'))].copy()

        # Initial subset of target data to experiments:
        target_245 = full_target_data[(full_target_data['model'] == esm) &
                                      (full_target_data['experiment'] == 'ssp245')].copy()
        # target SSP370 realizations:
        target_370 = full_target_data[(full_target_data['model'] == esm) &
                                      (full_target_data['experiment'] == 'ssp370')].copy()

        # Clean up target data
        if not target_245.empty:
            # clean up
            target_245 = prep_target_data(target_245).copy()

        if not target_370.empty:
            # clean up
            target_370 = prep_target_data(target_370).copy()

        # Not all models start the ensemble count at 1,
        # And not all experiments of a given model report the
        # same realizations.
        # select 5 ensemble realizations to
        # look at if there are more than 5.
        f = lambda x: x.ensemble[:x.idx]

        if not target_245.empty:
            ensemble_list = pd.DataFrame({'ensemble': target_245["ensemble"].unique()})
            ensemble_list['idx'] = ensemble_list['ensemble'].str.index('i')
            ensemble_list['ensemble_id'] = ensemble_list.apply(f, axis=1)
            ensemble_list['ensemble_id'] = ensemble_list['ensemble_id'].str[1:].astype(int)
            ensemble_list = ensemble_list.sort_values('ensemble_id').copy()
            if len(ensemble_list) > 5:
                ensemble_keep = ensemble_list.iloc[0:5].ensemble
            else:
                ensemble_keep = ensemble_list.ensemble

            target_245 = target_245[target_245['ensemble'].isin(ensemble_keep)].copy()
            del (ensemble_keep)
            del (ensemble_list)
        else:
            print('No target ssp245 data for ' + esm + '. Analysis will be skipped')

        if not target_370.empty:
            ensemble_list = pd.DataFrame({'ensemble': target_370["ensemble"].unique()})
            ensemble_list['idx'] = ensemble_list['ensemble'].str.index('i')
            ensemble_list['ensemble_id'] = ensemble_list.apply(f, axis=1)
            ensemble_list['ensemble_id'] = ensemble_list['ensemble_id'].str[1:].astype(int)
            ensemble_list = ensemble_list.sort_values('ensemble_id').copy()
            if len(ensemble_list) > 5:
                ensemble_keep = ensemble_list.iloc[0:5].ensemble
            else:
                ensemble_keep = ensemble_list.ensemble

            target_370 = target_370[target_370['ensemble'].isin(ensemble_keep)].copy()
            del (ensemble_keep)
            del (ensemble_list)

        else:
            print('No target ssp370 data for ' + esm + '. Analysis will be skipped')

        del(f)

        # Pull corresponding original data for these target runs.
        if not target_245.empty:
            orig_245  = get_orig_data(target_245)
        else:
            orig_245=[]

        if not target_370.empty:
            orig_370 = get_orig_data(target_370)
        else:
            orig_370=[]

        orig_245.append(orig_370).to_csv((OUTPUT_DIR + '/' + esm + '/' +
                                   'comparison_data_' + esm + '.csv'), index=False)


        # Use the match_neighborhood function to generate all of the matches between the target and
        # archive data points.
        if not target_245.empty:
            match_245_df = stitches.match_neighborhood(target_245, archive_data,
                                                       tol=tolerance)
        else:
            print('No target ssp245 data for ' + esm + '. Analysis will be skipped')

        if not target_370.empty:
            match_370_df = stitches.match_neighborhood(target_370, archive_data,
                                                       tol=tolerance)
        else:
            print('No target ssp370 data for ' + esm + '. Analysis will be skipped')

        for draw in range(0, Ndraws):
            # use the permute_stitching_recipes function to do the draws.
            # Make N_matches very large just so the full collapse free ensemble is generated
            if not target_245.empty:
                unformatted_recipe_245 = stitches.permute_stitching_recipes(N_matches=1,
                                                                            matched_data=match_245_df,
                                                                            archive=archive_data, testing=True)
            else:
                print('No target ssp245 data for ' + esm + '. Matching skipped')

            if not target_370.empty:
                unformatted_recipe_370 = stitches.permute_stitching_recipes(N_matches=1,
                                                                            matched_data=match_370_df,
                                                                            archive=archive_data, testing=True)
            else:
                print('No target ssp370 data for ' + esm + '. Matching skipped')

            # Clean up the recipe so that it can be used to generate the gridded data products.
            if not target_245.empty:
                recipe_245 = stitches.generate_gridded_recipe(unformatted_recipe_245)
                recipe_245.columns = ['target_start_yr', 'target_end_yr', 'archive_experiment', 'archive_variable',
                                      'archive_model', 'archive_ensemble', 'stitching_id', 'archive_start_yr',
                                      'archive_end_yr', 'tas_file']
                recipe_245.to_csv((OUTPUT_DIR + '/' + esm + '/' +
                                   'gridded_recipes_' + esm + '_target245' + '.csv'), index=False)
            else:
                print('No target ssp245 data for ' + esm + '. Recipe formatting skipped')

            if not target_370.empty:
                recipe_370 = stitches.generate_gridded_recipe(unformatted_recipe_370)
                recipe_370.columns = ['target_start_yr', 'target_end_yr', 'archive_experiment', 'archive_variable',
                                      'archive_model', 'archive_ensemble', 'stitching_id', 'archive_start_yr',
                                      'archive_end_yr', 'tas_file']
                recipe_370.to_csv((OUTPUT_DIR + '/' + esm + '/' +
                                   'gridded_recipes_' + esm + '_target370' + '.csv'), index=False)
            else:
                print('No target ssp370 data for ' + esm + '. Recipe formatting skipped')

            # stitch the GSAT values and save as csv
            try:
                if not target_245.empty:
                    gsat_245 = stitches.gmat_stitching(recipe_245)
                    for id in gsat_245.stitching_id.unique():
                        ds = gsat_245[gsat_245['stitching_id'] == id].copy()
                        fname = (OUTPUT_DIR + '/' + esm + '/' +
                                 'stitched_' + esm + '_GSAT_' + id + '.csv')
                        ds.to_csv(fname, index=False)
                else:
                    print('No target ssp245 data for ' + esm + '. GSAT stitching skipped')

                if not target_370.empty:
                    gsat_370 = stitches.gmat_stitching(recipe_370)
                    for id in gsat_370.stitching_id.unique():
                        ds = gsat_370[gsat_370['stitching_id'] == id].copy()
                        fname = (OUTPUT_DIR + '/' + esm + '/' +
                                 'stitched_' + esm + '_GSAT_' + id + '.csv')
                        ds.to_csv(fname, index=False)
                else:
                    print('No target ssp370 data for ' + esm + '. GSAT stitching skipped')

            # end try
            except:
                print(("Some issue stitching GMAT for " + esm + ". Skipping and moving on"))





        # end for loop over draws

    except:
        print(esm + ' failed. investigate offline')

# end for loop over ESMs
