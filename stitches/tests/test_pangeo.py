import pandas as pd
import unittest
import xarray as xr

from stitches.fx_pangeo import fetch_pangeo_table, fetch_nc

class TestPangeo(unittest.TestCase):

    def test_pangeo_fn(self):

        # Get the table of all pangeo contents as a data frame
        ptable = fetch_pangeo_table()
        self.assertEqual(type(ptable), pd.core.frame.DataFrame, 'fetch_pangeo_table did not return a data frame')

        # Select a single files from the pangeo cataog to import
        import_this = ptable.loc[(ptable["table_id"] == "Amon") & (ptable["activity_id"] == "ScenarioMIP")].reset_index(drop = True)
        path = import_this["zstore"][0]
        out = fetch_nc(path)
        self.assertEqual(type(out), xr.core.dataset.Dataset, 'problem with fetch_nc')

if __name__ == '__main__':
    unittest.main()
