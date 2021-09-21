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
OUTPUT_DIR = '/pic/projects/GCAM/stitches_pic/paper1_outputs'

# #############################################################################
# Experiment  setup
# #############################################################################
# experiment parameters
tolerance = 0.075
Ndraws = 1

# pangeo table of ESMs for reference
pangeo_path = pkg_resources.resource_filename('stitches', 'data/pangeo_table.csv')
pangeo_data = pd.read_csv(pangeo_path)
pangeo_data = pangeo_data[(pangeo_data['variable'] == 'tas') & (pangeo_data['domain'] == 'Amon') ].copy()

pangeo_126_esms = pangeo_data[(pangeo_data['experiment'] == 'ssp126')].model.unique().copy()
pangeo_126_esms.sort()
pangeo_585_esms = pangeo_data[(pangeo_data['experiment'] == 'ssp585')].model.unique().copy()
pangeo_585_esms.sort()


esms = pangeo_126_esms[15:,]
      # ['ACCESS-CM2', 'ACCESS-ESM1-5', 'AWI-CM-1-1-MR', 'BCC-CSM2-MR',
      #  'CAMS-CSM1-0', 'CAS-ESM2-0', 'CESM2', 'CESM2-WACCM',
      #  'CMCC-CM2-SR5', 'CMCC-ESM2', 'CanESM5', 'FGOALS-g3', 'FIO-ESM-2-0',
      #  'GISS-E2-1-G', 'HadGEM3-GC31-LL', 'HadGEM3-GC31-MM', 'IITM-ESM',
      #  'MCM-UA-1-0', 'MIROC-ES2L', 'MIROC6', 'MPI-ESM1-2-HR',
      #  'MPI-ESM1-2-LR', 'MRI-ESM2-0', 'NESM3', 'NorESM2-LM', 'NorESM2-MM',
      #  'TaiESM1', 'UKESM1-0-LL']
      # Skipping AWI and HadGEM LL so far - weird issues to dig into more.


# #############################################################################
# The experiment
# #############################################################################

# Load the full archive of all staggered windows, which we will be matching on
full_archive_path = pkg_resources.resource_filename('stitches', 'data/matching_archive_staggered.csv')
full_archive_data = pd.read_csv(full_archive_path)

# Load the original archive without staggered windows, which we will draw
# the target trajectories from for matching
full_target_path = pkg_resources.resource_filename('stitches', 'data/matching_archive.csv')
full_target_data = pd.read_csv(full_target_path)


# for each of the esms in the experiment, subset to what we want
# to work with and run the experiment.
for esm in esms:
    print(esm)

    # subset the archive and the targets to this ESM
    archive_data = full_archive_data[(full_archive_data['model'] == esm) &
                                     ((full_archive_data['experiment'] == 'ssp126') |
                                      (full_archive_data['experiment'] == 'ssp585'))].copy()

    target_245 = full_target_data[(full_target_data['model'] == esm) &
                                  (full_target_data['experiment'] == 'ssp245')].copy()

    target_370 = full_target_data[(full_target_data['model'] == esm) &
                                  (full_target_data['experiment'] == 'ssp370')].copy()

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
            unformatted_recipe_245 = stitches.permute_stitching_recipes(N_matches=10000,
                                                                        matched_data=match_245_df,
                                                                        archive=archive_data, testing=True)
        else:
            print('No target ssp245 data for ' + esm + '. Matching skipped')

        if not target_370.empty:
            unformatted_recipe_370 = stitches.permute_stitching_recipes(N_matches=10000,
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
            recipe_245.to_csv((OUTPUT_DIR + '/' + esm + '/experiment_scenarioMIP/' +
                               'gridded_recipes_' + esm + '_target245' + '.csv'), index=False)
        else:
            print('No target ssp245 data for ' + esm + '. Recipe formatting skipped')

        if not target_370.empty:
            recipe_370 = stitches.generate_gridded_recipe(unformatted_recipe_370)
            recipe_370.columns = ['target_start_yr', 'target_end_yr', 'archive_experiment', 'archive_variable',
                                  'archive_model', 'archive_ensemble', 'stitching_id', 'archive_start_yr',
                                  'archive_end_yr', 'tas_file']
            recipe_370.to_csv((OUTPUT_DIR + '/' + esm + '/experiment_scenarioMIP/' +
                               'gridded_recipes_' + esm + '_target370' + '.csv'), index=False)
        else:
            print('No target ssp370 data for ' + esm + '. Recipe formatting skipped')

        # stitch the GSAT values and save as csv
        try:
            if not target_245.empty:
                gsat_245 = stitches.gmat_stitching(recipe_245)
                for id in gsat_245.stitching_id.unique():
                    ds = gsat_245[gsat_245['stitching_id'] == id].copy()
                    fname = (OUTPUT_DIR + '/' + esm + '/experiment_scenarioMIP/' +
                             'stitched_' + esm + '_GSAT_' + id + '.csv')
                    ds.to_csv(fname, index=False)
            else:
                print('No target ssp245 data for ' + esm + '. GSAT stitching skipped')

            if not target_370.empty:
                gsat_370 = stitches.gmat_stitching(recipe_370)
                for id in gsat_370.stitching_id.unique():
                    ds = gsat_370[gsat_370['stitching_id'] == id].copy()
                    fname = (OUTPUT_DIR + '/' + esm + '/experiment_scenarioMIP/' +
                             'stitched_' + esm + '_GSAT_' + id + '.csv')
                    ds.to_csv(fname, index=False)
            else:
                print('No target ssp370 data for ' + esm + '. GSAT stitching skipped')

        #end try
        except:
            print(("Some issue stitching GMAT for " + esm +". Skipping and moving on"))


        # form and output the global gridded stitched products
        try:
            if not target_245.empty:
                outputs = stitches.gridded_stitching((OUTPUT_DIR + '/' + esm + '/experiment_scenarioMIP'), recipe_245)
            else:
                print('No target ssp245 data for ' + esm + '. Gridded stitching skipped')

            if not target_370.empty:
                outputs = stitches.gridded_stitching((OUTPUT_DIR + '/' + esm + '/experiment_scenarioMIP'), recipe_370)
            else:
                print('No target ssp370 data for ' + esm + '. Gridded stitching skipped')

        except:
            print(("Some issue stitching gridded for " + esm + ". Skipping and moving on"))

    # end for loop over draws
# end for loop over ESMs




