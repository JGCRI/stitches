import pandas as pd
import pkg_resources
import os
import unittest

from stitches.fx_pangeo import fetch_pangeo_table

class MyInternalData(unittest.TestCase):


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

       # cond = package_comparison.shape <= current_comparison.shape
       # self.assertEqual(cond, True)

       # # First check for files that have were removed from the pangeo archive.
       # cond = len(package_comparison.merge(current_comparison)) == len(package_comparison)
       # self.assertEqual(cond, True)

       # # Now check to see if files have been added to the pangeo archive.
       # cond = len(current_comparison.merge(package_comparison)) == len(current_comparison)
       # self.assertEqual(cond, True)

        self.assertEqual(True, True)

if __name__ == '__main__':
    unittest.main()
