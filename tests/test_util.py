import os
import unittest

import numpy as np
import pandas as pd

from stitches.fx_util import (
    anti_join,
    check_columns,
    combine_df,
    list_files,
    load_data_files,
    nrow,
    selstr,
)


class TestUtil(unittest.TestCase):
    """Unit tests for utility functions in the `stitches` package."""
    TABLE_A = pd.DataFrame(data={"col1": [1, 2, 3, 4], "col2": [3, 4, 5, 6]})
    TABLE_B = pd.DataFrame(data={"col1": [3, 4, 9], "col2": [5, 6, 10]})
    TABLE_C = pd.DataFrame(data={"col1": ["a", "b"], "col2": [4, 4]})
    TABLE_D = pd.DataFrame(data={"col3": ["a", "b", "c"], "col4": [3, 4, 5]})

    def test_check_columns(self):
        """Test the check_columns function for proper type checking."""
        self.assertTrue(
            isinstance(check_columns(self.TABLE_A, set(self.TABLE_A)), type(None))
        )
        with self.assertRaises(TypeError):
            check_columns(self.TABLE_A, self.TABLE_A.columns)
        with self.assertRaises(TypeError):
            check_columns(self.TABLE_A, {"fake"})

    def test_nrow(self):
        """Test the `nrow` function to ensure it returns the correct number of rows."""
        self.assertTrue(nrow(self.TABLE_A), 4)
        doubble_A = pd.concat([self.TABLE_A, self.TABLE_A])
        self.assertTrue(nrow(doubble_A), nrow(self.TABLE_A) * 2)

    def test_selstr(self):
        """Test the `selstr` function for substring extraction."""
        self.assertEqual(selstr("abcd", 0, 2), "ab")
        self.assertEqual(selstr("abcd", 0, 1), "a")
        with self.assertRaises(TypeError):
            selstr(123, 1, 2)
        with self.assertRaises(TypeError):
            selstr({"abcd", "abcd"}, 1, 2)

    def test_anti_join(self):
        """
        Test the `anti_join` function to ensure it returns the correct DataFrame.

        This test verifies that the `anti_join` function correctly returns a DataFrame
        that contains only the rows from the first input DataFrame that do not have
        matching key values in the second input DataFrame.
        """
        tableA = pd.DataFrame(
            np.random.rand(4, 3), pd.Index(list("abcd"), name="Key"), ["A", "B", "C"]
        ).reset_index()
        tableB = pd.DataFrame(
            np.random.rand(4, 3), pd.Index(list("aecf"), name="Key"), ["A", "B", "C"]
        ).reset_index()
        tableC = anti_join(tableA, tableB, bycols=["Key"])
        self.assertEqual(tableC.shape, (2, 4), "unexpected shape returned by anti_join")

        tableD = anti_join(tableB, tableA, bycols=["Key"])
        self.assertEqual(tableD.shape, (2, 4), "unexpected shape returned by anti_join")

    def test_combine_df(self):
        """Test the `combine_df` function for DataFrame combination."""
        TABLE_E = combine_df(self.TABLE_C, self.TABLE_D)
        self.assertEqual(nrow(TABLE_E), nrow(self.TABLE_C) * nrow(self.TABLE_D))
        with self.assertRaises(TypeError):
            combine_df(self.TABLE_C, self.TABLE_C)

    def test_file_fxns(self):
        """
        Test the `list_files` and `load_files` functions.
        """

        # Make sure that all of the files returned by the list_files function are all true.
        test_dir = "."
        out = list_files(test_dir)
        self.assertTrue(all(list(map(os.path.exists, out))))

        # test failure
        with self.assertRaises(TypeError):
            load_data_files("fake")


if __name__ == "__main__":
    unittest.main()
