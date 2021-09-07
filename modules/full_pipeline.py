# An example workflow of how to use the stitches package to go from
# picking out a target to the gridded products all within a single script.

# #############################################################################
# General setup
# #############################################################################
# Import packages
import pandas as pd
import stitches as stitches
import pkg_resources

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



