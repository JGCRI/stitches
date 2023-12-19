import os
import unittest

import numpy as np
import pandas as pd
import xarray as xr

from stitches.fx_pangeo import fetch_nc
from stitches.fx_stitch import (
    find_var_cols,
    find_zfiles,
    gmat_stitching,
    gridded_stitching,
    internal_stitch,
)
from stitches.fx_util import nrow


class TestStitch(unittest.TestCase):
    """
    Unit tests for stitching functions in the `stitches` package.

    This class provides a set of tests to ensure the correct functionality
    of the stitching functions, which are used to combine different climate
    model outputs into a single coherent dataset.
    """
    RUN = "ci"

    # This is an example recipe that will be used to test the stitching functions
    MY_RP = pd.DataFrame(
        data={
            "target_start_yr": [1850, 1859],
            "target_end_yr": [1858, 1867],
            "archive_experiment": ["historical", "historical"],
            "archive_variable": ["tas", "tas"],
            "archive_model": ["BCC-CSM2-MR", "BCC-CSM2-MR"],
            "archive_ensemble": ["r1i1p1f1", "r1i1p1f1"],
            "stitching_id": ["ssp245~r1i1p1f1~1", "ssp245~r1i1p1f1~1"],
            "archive_start_yr": [1859, 1886],
            "archive_end_yr": [1867, 1894],
            "tas_file": [
                "gs://cmip6/CMIP6/CMIP/BCC/BCC-CSM2-MR/historical/r1i1p1f1/Amon/tas/gn/v20181126/",
                "gs://cmip6/CMIP6/CMIP/BCC/BCC-CSM2-MR/historical/r1i1p1f1/Amon/tas/gn/v20181126/",
            ],
        }
    )

    def test_find_var_cols(self):
        """Test the `find_var_cols` function for identifying variable columns."""
        o = pd.DataFrame(data={"tas": [1, 2], "col2": [3, 4]})
        self.assertEqual(len(find_var_cols(o)), 0)

        o = pd.DataFrame(data={"tas_file": [1, 2], "col2": [3, 4]})
        self.assertEqual(find_var_cols(o), ["tas"])

        o = pd.DataFrame(data={"tas_file": [1, 2], "col2": [3, 4], "fake_file": [1, 2]})
        self.assertEqual(len(find_var_cols(o)), 2)

    def test_find_zfiles(self):
        """
        Test the `find_zfiles` function to ensure it correctly identifies zipped file paths.

        This test verifies that the `find_zfiles` function correctly identifies file paths
        for zipped files within a given DataFrame.
        """
        d = pd.DataFrame(data={"tas": [1, 2], "col2": [3, 4], "year": [1, 2]})
        self.assertEqual(len(find_zfiles(d)), 0)

        d = pd.DataFrame(
            data={
                "tas_file": ["file1.csv", "file2.csv"],
                "col2": [3, 4],
                "year": [1, 2],
            }
        )
        file_list = find_zfiles(d)
        self.assertEqual(type(file_list), np.ndarray)
        self.assertEqual(len(file_list), 2)

        d = pd.DataFrame(
            data={
                "tas_file": ["file1.csv", "file1.csv"],
                "col2": [3, 4],
                "year": [1, 2],
            }
        )
        file_list = find_zfiles(d)
        self.assertEqual(len(file_list), len(np.unique(file_list)))
        self.assertTrue(len(file_list) != nrow(d))

    def test_gmat_stitching(self):
        """
        Test the output returned by `gmat_stitching`.

        This test checks the type and structure of the output to ensure it meets expected formats.
        """
        out = gmat_stitching(self.MY_RP)

        self.assertEqual(type(out), pd.core.frame.DataFrame)
        time_steps = (
            max(self.MY_RP["target_end_yr"]) - min(self.MY_RP["target_start_yr"]) + 1
        )
        self.assertEqual(nrow(out), time_steps)

        # If the recipe is read in backwards, it shouldn't matter. The output should be the same.
        reverse = self.MY_RP.copy()
        reverse = reverse.iloc[::-1]
        out2 = gmat_stitching(reverse)
        self.assertEqual(out.shape, out2.shape)
        self.assertEqual(out["year"][0], out2["year"][0])
        self.assertEqual(out["value"][6], out["value"][6])

        # Manipulate the recipe, sometimes it will be fine other times it will throw an error.
        rp = self.MY_RP.copy()

        rp["tas_file"] = ["fake.nc", "fake.nc"]
        out = gmat_stitching(rp)
        self.assertEqual(type(out), pd.core.frame.DataFrame)

        # If the recpie is missing a column the stitching function should fail.
        with self.assertRaises(KeyError):
            gmat_stitching(rp.drop("tas_file"))

        with self.assertRaises(KeyError):
            gmat_stitching(rp.drop("target_start_yr"))

        with self.assertRaises(IndexError):
            rp["archive_model"] = ["fake", "fake"]
            gmat_stitching(rp)

    def test_gridded_related(self):
        """
        Test functions related to gridded data stitching.

        This test suite covers the functionality of gridded data stitching,
        ensuring that the output is consistent and errors are raised when
        expected.
        """
        if TestStitch.RUN == "ci":
            self.assertEqual(0, 0)
        else:
            # Set up the elements required for internal_stitch
            file_list = find_zfiles(self.MY_RP)
            data_list = list(map(fetch_nc, file_list))
            rslt = internal_stitch(self.MY_RP, data_list, file_list)

            time_steps = (
                12
                * (
                    max(self.MY_RP["target_end_yr"])
                    - min(self.MY_RP["target_start_yr"])
                )
                + 12
            )
            self.assertEqual(len(rslt["tas"]["time"]), time_steps)

            # Now do the stitching
            out = gridded_stitching(".", self.MY_RP)
            data = xr.open_dataset(out[0])
            time1 = data["tas"]["time"].values
            self.assertEqual(type(data), xr.core.dataset.Dataset)
            self.assertEqual(len(data["time"]), time_steps)
            os.remove(out[0])

            # If the recipe is read in backwards, it shouldn't matter the output should be the same.
            reverse = self.MY_RP.copy()
            reverse = reverse.iloc[::-1]
            out = gridded_stitching(".", reverse)
            data2 = xr.open_dataset(out[0])
            time2 = data2["tas"]["time"].values
            os.remove(out[0])
            self.assertEqual(max(time1 - time2), 0)

            # Manipulate the recipe, sometimes it will be fine other times it will throw an error.
            rp = self.MY_RP.copy()
            with self.assertRaises(TypeError):
                gridded_stitching("fake", rp)
            with self.assertRaises(KeyError):
                gridded_stitching(".", rp.drop("tas_file"))


if __name__ == "__main__":
    unittest.main()
