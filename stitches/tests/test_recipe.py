import pandas as pd
import pkg_resources
import unittest
from stitches.fx_util import check_columns
from stitches.fx_recepie import get_num_perms, remove_duplicates, make_recipe


class TestRecipe(unittest.TestCase):
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


if __name__ == '__main__':
    unittest.main()
