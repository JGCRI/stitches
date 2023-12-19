import unittest

import pandas as pd

from stitches.fx_match import (
    drop_hist_false_duplicates,
    internal_dist,
    match_neighborhood,
    shuffle_function,
)
from stitches.fx_util import nrow, remove_obs_from_match


class TestMatch(unittest.TestCase):
    def test_match_fxns(self):
        """
        Test the `match_neighborhood` function.

        This test ensures that the `match_neighborhood` function
        operates correctly.
        """
        # Read in some made up target data.
        path = "test-target_dat.csv"
        data = pd.read_csv(path)

        # Start by checking on a self test & without the drop hist constraint.
        self_match = match_neighborhood(data, data, tol=0)
        self.assertEqual(data.shape[0], self_match.shape[0])

        # Now try matching with the test archive
        path = "test-archive_dat.csv"
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
        path = "test-match_w_dup.csv"
        match_data = pd.read_csv(path)
        cleaned = drop_hist_false_duplicates(match_data)
        self.assertTrue(nrow(match_data) > nrow(cleaned))
        hist_target_experiments = cleaned[cleaned["target_start_yr"] <= 2020][
            "target_experiment"
        ].unique()
        self.assertEqual(len(hist_target_experiments), 1)


if __name__ == "__main__":
    unittest.main()
