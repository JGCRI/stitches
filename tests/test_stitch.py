"""Tests for the stitching functions.

Capability notes:

* ``find_var_cols`` / ``find_zfiles`` are pure DataFrame helpers and run offline.
* ``gmat_stitching`` reads the per-model ``tas-data`` CSVs from the installed
  package data, so it is marked ``package_data``.
* ``gridded_stitching`` and ``internal_stitch`` pull zarr stores from Pangeo and
  write NetCDF, so they are marked ``network`` and ``slow``.

The previous version of this module gated the gridded tests behind a
``RUN = "ci"`` class attribute whose false branch asserted ``0 == 0``. That made
them report as passing in CI while exercising nothing. They now skip visibly;
run them with ``pytest --network --slow``.
"""

import numpy as np
import pandas as pd
import pytest
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

# An example recipe used across the stitching tests.
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


@pytest.fixture
def recipe():
    """Return a fresh copy of the example recipe so tests cannot mutate shared state."""
    return MY_RP.copy()


# ---------------------------------------------------------------------------
# Offline helpers
# ---------------------------------------------------------------------------


def test_find_var_cols():
    """`find_var_cols` identifies columns whose names end in ``_file``."""
    o = pd.DataFrame(data={"tas": [1, 2], "col2": [3, 4]})
    assert len(find_var_cols(o)) == 0

    o = pd.DataFrame(data={"tas_file": [1, 2], "col2": [3, 4]})
    assert find_var_cols(o) == ["tas"]

    o = pd.DataFrame(data={"tas_file": [1, 2], "col2": [3, 4], "fake_file": [1, 2]})
    assert len(find_var_cols(o)) == 2


def test_find_zfiles_returns_empty_without_file_columns():
    """`find_zfiles` returns nothing when the frame has no ``_file`` columns."""
    d = pd.DataFrame(data={"tas": [1, 2], "col2": [3, 4], "year": [1, 2]})
    assert len(find_zfiles(d)) == 0


def test_find_zfiles_collects_paths():
    """`find_zfiles` returns an ndarray of the referenced file paths."""
    d = pd.DataFrame(
        data={"tas_file": ["file1.csv", "file2.csv"], "col2": [3, 4], "year": [1, 2]}
    )
    file_list = find_zfiles(d)

    assert isinstance(file_list, np.ndarray)
    assert len(file_list) == 2


def test_find_zfiles_deduplicates():
    """`find_zfiles` collapses repeated paths so each store is fetched once."""
    d = pd.DataFrame(
        data={"tas_file": ["file1.csv", "file1.csv"], "col2": [3, 4], "year": [1, 2]}
    )
    file_list = find_zfiles(d)

    assert len(file_list) == len(np.unique(file_list))
    assert len(file_list) != nrow(d)


# ---------------------------------------------------------------------------
# Global-mean stitching (needs the installed tas-data archive)
# ---------------------------------------------------------------------------


@pytest.mark.package_data
def test_gmat_stitching_shape(package_data, recipe):
    """`gmat_stitching` returns one row per target year covered by the recipe."""
    out = gmat_stitching(recipe)

    assert isinstance(out, pd.DataFrame)

    time_steps = max(recipe["target_end_yr"]) - min(recipe["target_start_yr"]) + 1
    assert nrow(out) == time_steps


@pytest.mark.package_data
def test_gmat_stitching_is_row_order_invariant(package_data, recipe):
    """Reversing the recipe row order must not change the stitched result."""
    out = gmat_stitching(recipe)

    reverse = recipe.iloc[::-1]
    out2 = gmat_stitching(reverse)

    assert out.shape == out2.shape
    pd.testing.assert_frame_equal(
        out.sort_values("year").reset_index(drop=True),
        out2.sort_values("year").reset_index(drop=True),
    )


@pytest.mark.package_data
def test_gmat_stitching_ignores_tas_file_column(package_data, recipe):
    """The ``tas_file`` values are unused by `gmat_stitching`, which reads local data."""
    recipe["tas_file"] = ["fake.nc", "fake.nc"]
    out = gmat_stitching(recipe)

    assert isinstance(out, pd.DataFrame)


@pytest.mark.package_data
@pytest.mark.parametrize("missing", ["tas_file", "target_start_yr"])
def test_gmat_stitching_requires_columns(package_data, recipe, missing):
    """Dropping a required recipe column raises `KeyError`."""
    with pytest.raises(KeyError):
        gmat_stitching(recipe.drop(columns=missing))


@pytest.mark.package_data
def test_gmat_stitching_rejects_unknown_model(package_data, recipe):
    """A model absent from the archive raises `IndexError`."""
    recipe["archive_model"] = ["fake", "fake"]
    with pytest.raises(IndexError):
        gmat_stitching(recipe)


# ---------------------------------------------------------------------------
# Gridded stitching (needs Pangeo)
# ---------------------------------------------------------------------------


def _expected_monthly_steps(rp):
    """Return the number of monthly time steps a recipe should produce."""
    return 12 * (max(rp["target_end_yr"]) - min(rp["target_start_yr"])) + 12


@pytest.mark.network
@pytest.mark.slow
def test_internal_stitch_time_length(recipe):
    """`internal_stitch` produces the expected number of monthly time steps."""
    file_list = find_zfiles(recipe)
    data_list = list(map(fetch_nc, file_list))
    rslt = internal_stitch(recipe, data_list, file_list)

    assert len(rslt["tas"]["time"]) == _expected_monthly_steps(recipe)


@pytest.mark.network
@pytest.mark.slow
def test_gridded_stitching_writes_dataset(tmp_path, recipe):
    """`gridded_stitching` writes a NetCDF file with the expected time axis."""
    out = gridded_stitching(str(tmp_path), recipe)

    with xr.open_dataset(out[0]) as data:
        assert isinstance(data, xr.Dataset)
        assert len(data["time"]) == _expected_monthly_steps(recipe)


@pytest.mark.network
@pytest.mark.slow
def test_gridded_stitching_is_row_order_invariant(tmp_path, recipe):
    """Reversing the recipe row order must not change the stitched time axis."""
    forward = gridded_stitching(str(tmp_path / "fwd"), recipe)
    with xr.open_dataset(forward[0]) as data:
        time1 = data["tas"]["time"].values

    reverse = gridded_stitching(str(tmp_path / "rev"), recipe.iloc[::-1])
    with xr.open_dataset(reverse[0]) as data2:
        time2 = data2["tas"]["time"].values

    assert max(time1 - time2) == 0


@pytest.mark.network
@pytest.mark.slow
def test_gridded_stitching_rejects_bad_output_dir(recipe):
    """A nonexistent output directory raises `TypeError`."""
    with pytest.raises(TypeError):
        gridded_stitching("fake", recipe)


@pytest.mark.network
@pytest.mark.slow
def test_gridded_stitching_requires_file_column(tmp_path, recipe):
    """Dropping ``tas_file`` raises `KeyError`."""
    with pytest.raises(KeyError):
        gridded_stitching(str(tmp_path), recipe.drop(columns="tas_file"))
