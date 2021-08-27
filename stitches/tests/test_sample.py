# TODO CRV does the test file need to be broken up? if so what changes need to be made to the
# TODO CRV untitest.main call?
import pandas as pd
import unittest

from stitches.fx_util import join_exclude


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



if __name__ == '__main__':
    unittest.main()
