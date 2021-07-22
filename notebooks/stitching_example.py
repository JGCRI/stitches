# Right now this is just and example of the framework we have working in python.

# #############################################################################
# General setup
# #############################################################################
# Import packages
import pandas as pd
import xarray as xr
import stitches.fx_stitch as pkg

# This requires that the recipe that has already been generated.
local = '/Users/dorh012/projects/2021/stitches/notebooks/stitches_dev/'
recipe = pd.read_csv(local+'MPI_gridded_recipes_for_python.csv')

# For now select a single id to stitch.
id = recipe['stitching_id'].unique()[0]
rp = recipe[recipe['stitching_id'] == id].copy()

# Now do the stitching!
out = pkg.stitching(out_dir='./', rp=rp)


# Open the netcdf files.
out_tas = xr.open_dataset(out[0])
out_pr = xr.open_dataset(out[1])
out_psl = xr.open_dataset(out[2])


# Make some plots
out_tas.tas.sel(time='2050-01').squeeze().plot()


