
import pandas as pd
import pkg_resources
import os
import unittest
import xarray as xr

from stitches.fx_stitch import find_var_cols, find_zfiles, internal_stitch, gmat_stitching, gridded_stitching
from stitches.fx_pangeo import  fetch_nc

class TestStitch(unittest.TestCase):

    def test_stitch_fxns(self):

        # Read in a small recipe, only for 2 period of time.
        path = pkg_resources.resource_filename('stitches', 'tests/test-recipe_dat.csv')
        recipe = pd.read_csv(path)

        """Test find_var_cols."""
        # This is a tas recipe we know that the length of the variable should equal 1.
        variables = find_var_cols(recipe)
        self.assertEqual(len(variables), 1)

        """Test find_zfiles"""
        # The length of the files should be equal to the files used in the recipe.
        file_list = find_zfiles(recipe)
        self.assertEqual(len(file_list), len(recipe['tas_file'].unique()))

        """Test internal_stitch"""
        # Download all of the data from pangeo.
        data_list = list(map(fetch_nc, file_list))
        self.assertEqual(len(file_list), len(data_list))

        # Test the internal stitching function
        rslt = internal_stitch(recipe, data_list, file_list)
        self.assertTrue(type(rslt) is dict)
        time_steps = 12 * (max(recipe['target_end_yr']) - min(recipe['target_start_yr'])) + 12
        self.assertEqual(len(rslt['tas']['time']), time_steps)

        """Test gridded_stitching"""
        # Test the gridded stitch function
        # TODO CRV is there a better way to do this? I think ideally I would like to write
        # to some temporary directory
        out = gridded_stitching(".", recipe)
        data = xr.open_dataset(out[0])
        self.assertEqual(type(data), xr.core.dataset.Dataset)
        self.assertEqual(len(data["time"]), time_steps)
        os.remove(out[0])

        """Test gmat_stitching"""
        # Test the global mean stitch function.
        out = gmat_stitching(recipe)
        time_steps = max(recipe['target_end_yr']) - min(recipe['target_start_yr']) + 1
        self.assertEqual(out.shape[0], time_steps)


if __name__ == '__main__':
    unittest.main()
