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
        out = load_data_files("tests")
        self.assertTrue(type(out) == pd.core.frame.DataFrame)
        #self.assertRaises(TypeError, load_data_files("fake"))

        """test combine_df"""
        d = {'col1': ["a", "b"], 'col2': [4, 4]}
        tableA = pd.DataFrame(d)

        d = {'col3': ["a", "b", "c"], 'col4': [3, 4, 5]}
        tableB = pd.DataFrame(d)

        tableC = combine_df(tableA, tableB)
        self.assertEqual(nrow(tableC), nrow(tableA) * nrow(tableB))

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

        "Test the remove_obs_from_match function"
        out1 = remove_obs_from_match(match1, match1.iloc[[2]])
        self.assertTrue(nrow(out1) < nrow(match1))

        "Test internal shuffle fxn"
        subset_data = data.head(10)
        out = shuffle_function(subset_data)
        self.assertTrue(subset_data.shape == out.shape)
        self.assertTrue(abs(subset_data.year - out.year).mean() > 0)

        "Test internal_dist"
        # Test against self, it should return exactly 1 entry & the distance values calculated
        # should be equal to 0, since calculating the distance against itself.
        dist1 = internal_dist(subset_data.fx[0], subset_data.dx[0], subset_data)
        self.assertTrue(nrow(dist1) == 1)
        self.assertTrue(dist1.dist_dx[0] == 0)
        self.assertTrue(dist1.dist_fx[0] == 0)
        self.assertTrue(dist1.dist_l2[0] == 0)

        "Test drop_hist_false_duplicates"
        # Read in the match test data.
        path = pkg_resources.resource_filename('stitches', 'tests/test-match_w_dup.csv')
        match_data = pd.read_csv(path)
        cleaned = drop_hist_false_duplicates(match_data)
        self.assertTrue(nrow(match_data) > nrow(cleaned))
        hist_target_experiments = cleaned[cleaned["target_start_yr"] <= 2020]["target_experiment"].unique()
        self.assertEqual(len(hist_target_experiments), 1)

    def test_recipe_fxns(self):

        # Read in the match test data.
        path = pkg_resources.resource_filename('stitches', 'tests/test-match_w_dup.csv')
        match_data = pd.read_csv(path)

        "Test get_num_perms"
        out = get_num_perms(match_data)
        self.assertEqual(type(out), list)
        # Throw an error if the output does not match what we would expect.
        check_columns(out[0], set(['target_variable', 'target_experiment', 'target_ensemble',
                                   'target_model', 'minNumMatches', 'totalNumPerms']))
        check_columns(out[1], set(['target_variable', 'target_experiment', 'target_ensemble',
                                   'target_model', 'target_start_yr', 'target_end_yr', 'target_year',
                                   'target_fx', 'target_dx', 'n_matches']))
        self.assertEqual(len(out), 2)

        "Test remove_duplicates"

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

    def test_pkg_data(self):

        # Make sure the matching data is consistent with internal tas data.
        #
        # Read in the archive that is used in the matching process, we are going to
        # compare the model / experiment / ensembles / variables that exist here with
        # must also exist in the raw tas data files.
        path = pkg_resources.resource_filename('stitches', 'data/matching_archive.csv')
        df = pd.read_csv(path)[["experiment", "model", "ensemble"]]
        matching_archive = df.drop_duplicates(keep='last')

        # Load the model / experiment / ensembles / variables for all of the files
        # in the stitches/data/tas directory.
        tas_dir = pkg_resources.resource_filename('stitches', 'data/tas-data')
        tas_files = os.listdir(tas_dir)
        tas_files = [s for s in tas_files if "csv" in s]

        internal_tas_list = []
        for f in tas_files:
            dat = pd.read_csv(tas_dir + "/" + f)[['experiment', 'ensemble', 'model']].drop_duplicates(keep='last').reset_index(drop=True)
            internal_tas_list.append(dat)
        internal_tas_data = pd.concat(internal_tas_list)

        # Make sure that the contents of the matching archive is consistent with all of the
        # package tas data.
        cond = len(matching_archive.merge(internal_tas_data)) == len(matching_archive)
        self.assertEqual(cond, True)

        # Okay now the stitches pangeo table against the current table, limit the data
        # to the experiment & ensemble data of thee files that have been included as
        # internal tas package data.
        exps_check = internal_tas_data["experiment"].unique()
        ens_check = internal_tas_data["ensemble"].unique()

        path = pkg_resources.resource_filename('stitches', 'data/pangeo_comparison_table.csv')
        package_comparison = pd.read_csv(path)
        package_comparison = package_comparison.loc[((package_comparison["experiment_id"].isin(exps_check)) &
                                                    (package_comparison["member_id"].isin(ens_check)))]
        current_comparison = fetch_pangeo_table()
        current_comparison = current_comparison.loc[((current_comparison["experiment_id"].isin(exps_check)) &
                                                    (current_comparison["member_id"].isin(ens_check)))]

        cond = package_comparison.shape <= current_comparison.shape
        self.assertEqual(cond, True)

        # First check for files that have were removed from the pangeo archive.
        cond = len(package_comparison.merge(current_comparison)) == len(package_comparison)
        self.assertEqual(cond, True)

        # Now check to see if files have been added to the pangeo archive.
        cond = len(current_comparison.merge(package_comparison)) == len(current_comparison)
        self.assertEqual(cond, True)

    def test_pangeo(self):
        out = fetch_pangeo_table()
        self.assertEqual(type(out), pd.core.frame.DataFrame)

        import_this = out.loc[(out["table_id"] == "Amon") & (out["activity_id"] == "ScenarioMIP")].reset_index(drop = True)
        path = import_this["zstore"][0]
        out = fetch_nc(path)
        self.assertEqual(type(out), xr.core.dataset.Dataset)

if __name__ == '__main__':
    unittest.main()
