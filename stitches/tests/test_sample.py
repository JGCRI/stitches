# TODO CRV does the test file need to be broken up? if so what changes need to be made to the
# TODO CRV untitest.main call?
# TODO CRV for the test data I am worried about where those files should be written out to

import numpy as np
import pandas as pd
import pkg_resources
import os
import unittest
import xarray as xr

from stitches.fx_match import match_neighborhood
from stitches.fx_pangeo import fetch_nc
from stitches.fx_recepie import remove_duplicates, make_recipe
from stitches.fx_stitch import find_var_cols, find_zfiles, internal_stitch, gmat_stitching, gridded_stitching
from stitches.fx_util import anti_join, join_exclude



class TestUtil(unittest.TestCase):

    def test_join_exclude(self):
        """Test to make sure `join_exclude` returns output as expected."""

        # If remove self then should should return an empty data frame.
        d = {'col1': [1, 2, 3, 4], 'col2': [3, 4, 5, 6]}
        df = pd.DataFrame(data=d)
        rm = pd.DataFrame(data=d)

        out1 = join_exclude(df, rm)
        self.assertEqual("<class 'pandas.core.frame.DataFrame'>", str(type(out1)))
        self.assertEqual(len(out1), 0)

        # we also expect an empty data frame when remove only contains 1 column
        rm = rm[['col1']]
        out2 = join_exclude(df, rm)
        self.assertEqual(len(out2), 0)

        # When there is only overlap of a few rows the output should only return the
        # number of rows the 2 data frames have in common and should not add an new
        # rows!
        r = {'col1': [3, 4, 9], 'col2': [5, 6, 10]}
        rm = pd.DataFrame(data=r)
        out3 = join_exclude(df, rm)
        self.assertEqual(out3.shape[0], 2)

    def test_anti_join(self):
        tableA = pd.DataFrame(np.random.rand(4, 3),
                              pd.Index(list('abcd'), name='Key'),
                              ['A', 'B', 'C']).reset_index()
        tableB = pd.DataFrame(np.random.rand(4, 3),
                              pd.Index(list('aecf'), name='Key'),
                              ['A', 'B', 'C']).reset_index()
        tableC = anti_join(tableA, tableB, bycols=['Key'])
        self.assertEqual(tableC.shape, (2, 4))

        tableD = anti_join(tableB, tableA, bycols=['Key'])
        self.assertEqual(tableD.shape, (2, 4))
        self.assertTrue(tableD.shape, (2, 4))

    def test_match_fxns(self):
        """Testing the `match_neighborhood` functions"""
        # Read in some made up target data.
        path = pkg_resources.resource_filename('stitches', 'tests/test-target_dat.csv')
        data = pd.read_csv(path)

        # Start by checking on a self test & without the drop hist constraint.
        self_match = match_neighborhood(data, data, tol=0)
        self.assertEqual(data.shape[0], self_match.shape[0])

        # Now try matching with the test archive
        path = pkg_resources.resource_filename('stitches', 'tests/test-archive_dat.csv')
        archive = pd.read_csv(path)
        match1 = match_neighborhood(data, archive, tol=0)
        # We know this must be true because the test archive contains duplicates of the
        # test data, but when we increase the tolerance there should be even more matches
        # in the data frame.
        self.assertEqual(match1.shape[0], data.shape[0] * 2)
        match2 = match_neighborhood(data, archive, tol=0.1)
        self.assertTrue(match2.shape[0] > data.shape[0])

    def test_recipe_fxns(self):
        # TODO this needs to be written but this is some what difficult to figure out how
        # TODO to actually these functions.
        self.assertEqual(True, True)


    def test_stitch_fxns(self):
        """Test the family of functions that are used in the stitching process."""

        # Read in a small recipe, only for 2 period of time.
        path = pkg_resources.resource_filename('stitches', 'tests/test-recipe_dat.csv')
        recipe = pd.read_csv(path)

        # This is a tas recipe we know that the length of the variable should equal 1.
        variables = find_var_cols(recipe)
        self.assertEqual(len(variables), 1)

        # The length of the files should be equal to the files used in the recipe.
        file_list = find_zfiles(recipe)
        self.assertEqual(len(file_list), len(recipe['tas_file'].unique()))

        # Download all of the data from pangeo.
        data_list = list(map(fetch_nc, file_list))
        self.assertEqual(len(file_list), len(data_list))

        # Test the internal stitching function
        rslt = internal_stitch(recipe, data_list, file_list)
        self.assertTrue(type(rslt) is dict)
        time_steps = 12 * (max(recipe['target_end_yr']) - min(recipe['target_start_yr'])) + 12
        self.assertEqual(len(rslt['tas']['time']), time_steps)

        # Test the gridded stitch function
        # TODO CRV is there a better way to do this? I think ideally I would like to write
        # to some temporary directory
        out = gridded_stitching(".", recipe)
        data = xr.open_dataset(out[0])
        self.assertEqual(type(data), xr.core.dataset.Dataset)
        self.assertEqual(len(data["time"]), time_steps)
        os.remove(out[0])

        # Test the global mean stitch function.
        out = gmat_stitching(recipe)
        time_steps = max(recipe['target_end_yr']) - min(recipe['target_start_yr']) + 1
        self.assertEqual(out.shape[0], time_steps)






if __name__ == '__main__':
    unittest.main()
