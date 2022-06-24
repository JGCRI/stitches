import pandas as pd
import unittest

from stitches.fx_pangeo import fetch_pangeo_table

class TestMyPangeo(unittest.TestCase):
    def test_fetch_pangeo(self):
        out = fetch_pangeo_table()
        self.assertEqual(type(out), pd.core.frame.DataFrame)


if __name__ == '__main__':
    unittest.main()
