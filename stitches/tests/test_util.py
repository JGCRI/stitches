import numpy as np
import pkg_resources
import pandas as pd
import os
import unittest
from stitches.fx_util import anti_join, nrow, check_columns, selstr, combine_df, list_files, load_data_files

class TestUtil(unittest.TestCase):

    TABLE_A = pd.DataFrame(data={'col1': [1, 2, 3, 4], 'col2': [3, 4, 5, 6]})
    TABLE_B = pd.DataFrame(data={'col1': [3, 4, 9], 'col2': [5, 6, 10]})
    TABLE_C = pd.DataFrame(data={'col1': ["a", "b"], 'col2': [4, 4]})
    TABLE_D = pd.DataFrame(data={'col3': ["a", "b", "c"], 'col4': [3, 4, 5]})

    def test_check_columns(self):
        self.assertTrue(type(check_columns(self.TABLE_A, set(self.TABLE_A))) is type(None))
        with self.assertRaises(TypeError):
            check_columns(self.TABLE_A, self.TABLE_A.columns)
        with self.assertRaises(TypeError):
            check_columns(self.TABLE_A, {'fake'})

    def test_nrow(self):
        self.assertTrue(nrow(self.TABLE_A), 4)
        doubble_A = pd.concat([self.TABLE_A, self.TABLE_A])
        self.assertTrue(nrow(doubble_A), nrow(self.TABLE_A) * 2)

    def test_selstr(self):
        self.assertEqual(selstr("abcd", 0, 2), 'ab')
        self.assertEqual(selstr("abcd", 0, 1), 'a')
        with self.assertRaises(TypeError):
            selstr(123, 1, 2)
        with self.assertRaises(TypeError):
            selstr({"abcd", "abcd"}, 1, 2)

    def test_anti_join(self):
        tableA = pd.DataFrame(np.random.rand(4, 3),
                              pd.Index(list('abcd'), name='Key'),
                              ['A', 'B', 'C']).reset_index()
        tableB = pd.DataFrame(np.random.rand(4, 3),
                              pd.Index(list('aecf'), name='Key'),
                              ['A', 'B', 'C']).reset_index()
        tableC = anti_join(tableA, tableB, bycols=['Key'])
        self.assertEqual(tableC.shape, (2, 4), 'unexpected shape returned by anti_join')

        tableD = anti_join(tableB, tableA, bycols=['Key'])
        self.assertEqual(tableD.shape, (2, 4), 'unexpected shape returned by anti_join')

    def test_combine_df(self):
        TABLE_E = combine_df(self.TABLE_C, self.TABLE_D)
        self.assertEqual(nrow(TABLE_E), nrow(self.TABLE_C) * nrow(self.TABLE_D))
        with self.assertRaises(TypeError):
            combine_df(self.TABLE_C, self.TABLE_C)

    def test_file_fxns(self):
        # Test the list files and load files function

        # Make sure that all of the files returned by the list_files function are all true.
        out = list_files(pkg_resources.resource_filename('stitches', 'tests'))
        self.assertTrue(all(list(map(os.path.exists, out))))

        # Make sure that we can load a csv data file
        self.assertTrue(type(load_data_files("tests")) == pd.core.frame.DataFrame)
        with self.assertRaises(TypeError):
            load_data_files("fake")


if __name__ == '__main__':
    unittest.main()
