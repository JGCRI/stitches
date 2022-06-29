import pandas as pd
import pkg_resources
import os
import unittest

from stitches.fx_util import nrow
from stitches.fx_pangeo import fetch_pangeo_table

class TestInternalData(unittest.TestCase):

    # This test checks the internal package data and throws an error if the
    # internal package data needs to be updated/rerun.
    PANGEO_TABLE = pd.read_csv(pkg_resources.resource_filename('stitches', 'data/pangeo_comparison_table.csv'))
    MATCHING_ARCHIVE = pd.read_csv(pkg_resources.resource_filename('stitches', 'data/matching_archive.csv'))

    # Get the most up todate pangeo catalog
    CURRENT_DATA = fetch_pangeo_table()

    def test_pangeo_catalog1(self):
        internal_fresh = self.PANGEO_TABLE.merge(self.CURRENT_DATA, how='left', indicator=True)
        new_data = internal_fresh[internal_fresh["_merge"] == 'left_only']
        self.assertEqual(nrow(new_data), 0, "New data available on Pangeo")
        new_tas_data = new_data[new_data["variable_id"] == "tas"]
        self.assertEqual(nrow(new_tas_data), 0, "New tas available on Pangeo")

    def test_pangeo_catalog2(self):
        fresh_internal = self.CURRENT_DATA.merge(self.PANGEO_TABLE, how='left', indicator=True)
        data_removed = fresh_internal[fresh_internal["_merge"] != "both"]
        self.assertEqual(nrow(data_removed), 0,
                         "Internal pangeo catalog contains content not in current pangeo catalog")

    def test_tas_files(self):
        # Make sure that the contents of the tas files are consistent with the
        # pangeo catalog. This test was added because there has been issues
        # with this in the past.

        # All of the tas files to import
        tas_dir = pkg_resources.resource_filename('stitches', 'data/tas-data')
        tas_files = os.listdir(tas_dir)
        tas_files = [s for s in tas_files if "csv" in s]

        # Read in the tas file information
        internal_tas_list = []
        for f in tas_files:
            dat = pd.read_csv(tas_dir + "/" + f)[['experiment', 'ensemble', 'model']]\
                .drop_duplicates(keep='last').reset_index(drop=True)
            internal_tas_list.append(dat)
        internal_tas_data = pd.concat(internal_tas_list)

        archive_meta_info = self.MATCHING_ARCHIVE[["experiment", "model", "ensemble"]]\
            .drop_duplicates(keep='last').reset_index(drop=True)

        archive_tas = archive_meta_info.merge(internal_tas_data, how='left', indicator=True)
        extra_data = archive_tas[archive_tas["_merge"] != "both"]
        self.assertEqual(nrow(extra_data), 0,
                         "Archive data contains data not found within tas data files")

if __name__ == '__main__':
    unittest.main()
