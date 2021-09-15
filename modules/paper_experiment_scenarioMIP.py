# Using SSP126 and SSP585 as bracketing scenarios,
# emulate  SSP245 and SSP370 as target scenarios.
# For every ESM with monthly tas available.
# Using tol=0.08degC (min maxTol found in offline experiments), to be
# safe since we haven't found maxTol for each ESM yet.

# #############################################################################
# General setup
# #############################################################################
# Import packages
import pandas as pd
import stitches as stitches
import pkg_resources

pd.set_option('display.max_columns', None)


# pangeo table of ESMs for reference
pangeo_path = pkg_resources.resource_filename('stitches', 'data/pangeo_table.csv')
pangeo_data = pd.read_csv(pangeo_path)
pangeo_data = pangeo_data[(pangeo_data['variable'] == 'tas') & (pangeo_data['domain'] == 'Amon') ].copy()

pangeo_126_esms = pangeo_data[(pangeo_data['experiment'] == 'ssp126')].model.unique().copy()
pangeo_126_esms.sort()
pangeo_585_esms = pangeo_data[(pangeo_data['experiment'] == 'ssp585')].model.unique().copy()
pangeo_585_esms.sort()

print(pd.DataFrame({'model_126' : pangeo_126_esms, 'model_585' : pangeo_585_esms}))

# esms = pangeo_126_esms
esms = ['ACCESS-CM2', 'ACCESS-ESM1-5', 'AWI-CM-1-1-MR', 'BCC-CSM2-MR',
       'CAMS-CSM1-0', 'CAS-ESM2-0', 'CESM2', 'CESM2-WACCM',
       'CMCC-CM2-SR5', 'CMCC-ESM2', 'CanESM5', 'FGOALS-g3', 'FIO-ESM-2-0',
       'GISS-E2-1-G', 'HadGEM3-GC31-LL', 'HadGEM3-GC31-MM', 'IITM-ESM',
       'MCM-UA-1-0', 'MIROC-ES2L', 'MIROC6', 'MPI-ESM1-2-HR',
       'MPI-ESM1-2-LR', 'MRI-ESM2-0', 'NESM3', 'NorESM2-LM', 'NorESM2-MM',
       'TaiESM1', 'UKESM1-0-LL']


# Load the archive data we want to match on.
archive_path = pkg_resources.resource_filename('stitches', 'data/matching_archive.csv')
data = pd.read_csv(archive_path)

# Select the some data to use as the target data
target_data = data[data["model"] == 'BCC-CSM2-MR']
target_data = target_data[target_data["experiment"] == 'ssp245']
target_data = target_data[target_data["ensemble"].isin(['r4i1p1f1', 'r1i1p1f1'])]
target_data = target_data.reset_index(drop=True)

# Select the data to use as our archive.
archive_data = data[data['model'] == 'BCC-CSM2-MR'].copy()

# Use the match_neighborhood function to generate all of the matches between the target and
# archive data points.
match_df = stitches.match_neighborhood(target_data, archive_data, tol=0.1)

# So the permute stitiching recipe works it works when N matches is set to 1 see blow for an
# example where the function fails to return 2 matches per target.
unformatted_recipe = stitches.permute_stitching_recipes(N_matches=4, matched_data=match_df, archive=archive_data)

# The next step is to clean up the recipe so that it can be used to generate the gridded data products.
# TODO this should be wrapped in a function that adds the other variables to the recpies
# TODO and the everything should be wrapped in a command that does the make grided recpie.
recipe = stitches.generate_gridded_recipe(unformatted_recipe)
recipe.columns = ['target_start_yr', 'target_end_yr', 'archive_experiment', 'archive_variable',
                  'archive_model', 'archive_ensemble', 'stitching_id', 'archive_start_yr',
                  'archive_end_yr', 'tas_file']

# Get the gridded stitching & the global mean stitched products!
outputs = stitches.gridded_stitching('.', recipe)
outputs = stitches.gmat_stitching(recipe)



