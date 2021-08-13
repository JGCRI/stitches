# Right now this is serving as a place to provide and example for
# to start working around with the stitching proccess but my
# guess is that this might be more of a notebook than a modeule...

# #############################################################################
# General setup
# #############################################################################
# Import packages
import pandas as pd

# This requires that the recpie has already been set up.
recipe = pd.read_csv('/Users/dorh012/projects/2021/stitches/notebooks/stitches_dev/MPI_gridded_recipes_for_python.csv')

id = recipe['stitching_id'].unique()[0]
rp = recipe[recipe['stitching_id'] == id].copy()



