# TODO CRV does the test file need to be broken up? if so what changes need to be made to the
# TODO CRV untitest.main call?
# TODO CRV for the test data I am worried about where those files should be written out to
# TODO are there ways to write a message related to what the problem is?
# TODO is there a place where scripts go to generating to generate test data?

import numpy as np
import pandas as pd
import pkg_resources
import os
import unittest
import xarray as xr

from stitches.fx_match import match_neighborhood, shuffle_function, internal_dist, drop_hist_false_duplicates
from stitches.fx_pangeo import fetch_nc, fetch_pangeo_table
from stitches.fx_recepie import get_num_perms, remove_duplicates, make_recipe
from stitches.fx_stitch import find_var_cols, find_zfiles, internal_stitch, gmat_stitching, gridded_stitching
from stitches.fx_util import anti_join, remove_obs_from_match, join_exclude, nrow, check_columns, selstr, list_files, load_data_files, combine_df



class TestUtil(unittest.TestCase):

    # TODO load the data that is used consistently in all the tests here?

    def test_util_fxns(self):
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

        """Test to make sure `anti_join` returns output as expected."""
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

        """test nrow function"""
        self.assertTrue(nrow(tableD), 2)

        """test check columns"""
        # TODO CRV how to I assert that an error is being thrown??
        #self.assertRaises(TypeError, check_columns(tableD, tableD.columns))
        self.assertTrue(type(check_columns(tableD, set(tableD.columns))) is type(None))
        #self.assertRaises(TypeError, check_columns(tableD, set('fake')))

        """Check selstr"""
        #self.assertRaises(TypeError, selstr(123, 1, 2))
        self.assertEqual(selstr("abcd", 0, 2), 'ab')
        self.assertEqual(selstr("abcd", 0, 1), 'a')

        """Test list_files"""
        # make sure that all of the files returned by the list_files function are all true.
        out = list_files(pkg_resources.resource_filename('stitches', 'tests'))
        self.assertTrue(all(list(map(os.path.exists, out))))

        """Test load_datafiles"""
        #out = load_data_files("tests")
        #self.assertTrue(type(out) == pd.core.frame.DataFrame)
        #self.assertRaises(TypeError, load_data_files("fake"))

        """test combine_df"""
        d = {'col1': ["a", "b"], 'col2': [4, 4]}
        tableA = pd.DataFrame(d)

        d = {'col3': ["a", "b", "c"], 'col4': [3, 4, 5]}
        tableB = pd.DataFrame(d)

        tableC = combine_df(tableA, tableB)
        self.assertEqual(nrow(tableC), nrow(tableA) * nrow(tableB))


if __name__ == '__main__':
    unittest.main()
