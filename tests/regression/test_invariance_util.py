"""Golden-output regression tests for the utility and data helpers.

Covers :mod:`stitches.fx_util` and :mod:`stitches.fx_data`. These functions are
small and cheap, but `global_mean` in particular carries real scientific weight:
it applies the cosine-of-latitude area weighting that converts a gridded field
into a global mean temperature. An error there would bias every emulated series
while still producing entirely plausible-looking numbers, so it is verified
against an analytically known result rather than only against a recorded one.
"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from stitches.fx_data import get_lat_name, global_mean
from stitches.fx_util import anti_join, combine_df, nrow, selstr

# Every test in this module is an output-invariance check; the CI regression job
# selects on this marker.
pytestmark = pytest.mark.regression


# ---------------------------------------------------------------------------
# fx_util
# ---------------------------------------------------------------------------


def test_selstr_extracts_substrings():
    """`selstr` slices a fixed character range out of a string."""
    assert selstr("abcdef", 0, 3) == "abc"
    assert selstr("abcdef", 2, 4) == "cd"
    assert selstr("abcdef", 0, 6) == "abcdef"


def test_nrow_counts_rows():
    """`nrow` reports the row count for frames and the length for arrays."""
    assert nrow(pd.DataFrame({"a": [1, 2, 3]})) == 3
    assert nrow(pd.DataFrame({"a": []})) == 0
    assert nrow(np.array([1, 2, 3, 4])) == 4


def test_combine_df_invariant(golden):
    """`combine_df` cross-join output is unchanged."""
    left = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    right = pd.DataFrame({"c": [10, 20, 30]})

    out = combine_df(left, right)
    golden.assert_frame(out, "util/combine_df", tolerance="exact")


def test_combine_df_produces_cartesian_product():
    """`combine_df` yields one row per input pair."""
    left = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    right = pd.DataFrame({"c": [10, 20, 30]})

    out = combine_df(left, right)

    assert len(out) == len(left) * len(right)


def test_combine_df_rejects_shared_columns():
    """Overlapping column names are rejected rather than silently suffixed."""
    frame = pd.DataFrame({"a": [1, 2]})

    with pytest.raises(Exception):
        combine_df(frame, frame)


def test_anti_join_invariant(golden):
    """`anti_join` output is unchanged."""
    left = pd.DataFrame({"k": [1, 2, 3, 4], "v": ["a", "b", "c", "d"]})
    right = pd.DataFrame({"k": [2, 4], "v": ["b", "d"]})

    out = anti_join(left, right, bycols=["k"])
    golden.assert_frame(out, "util/anti_join", tolerance="exact")


def test_anti_join_keeps_only_unmatched_rows():
    """`anti_join` retains exactly the left rows with no match on the right."""
    left = pd.DataFrame({"k": [1, 2, 3, 4], "v": ["a", "b", "c", "d"]})
    right = pd.DataFrame({"k": [2, 4], "v": ["b", "d"]})

    out = anti_join(left, right, bycols=["k"])

    assert sorted(out["k"].tolist()) == [1, 3]


def test_anti_join_with_no_overlap_returns_left():
    """With no shared keys, every left row survives."""
    left = pd.DataFrame({"k": [1, 2], "v": ["a", "b"]})
    right = pd.DataFrame({"k": [98, 99], "v": ["y", "z"]})

    out = anti_join(left, right, bycols=["k"])

    assert len(out) == len(left)


# ---------------------------------------------------------------------------
# fx_data
# ---------------------------------------------------------------------------


def _grid(lat_name="lat", nlat=19, nlon=36, ntime=3):
    """Build a small synthetic gridded dataset with a regular lat/lon grid.

    :param lat_name: Name to give the latitude coordinate, so the ``lat`` and
        ``latitude`` spellings can both be exercised.
    :return: A Dataset with a single ``tas`` variable.
    :rtype: xarray.Dataset
    """
    lats = np.linspace(-90, 90, nlat)
    lons = np.linspace(0, 350, nlon)
    times = pd.date_range("2000-01-01", periods=ntime, freq="MS")

    # A latitude-dependent field: warm at the equator, cold at the poles. This
    # makes the area weighting matter -- an unweighted mean would over-count the
    # poles, where grid cells are much smaller.
    field = np.empty((ntime, nlat, nlon))
    for t in range(ntime):
        field[t] = np.cos(np.deg2rad(lats))[:, None] * np.ones((1, nlon)) + t

    return xr.Dataset(
        {"tas": ((("time", lat_name, "lon")), field)},
        coords={"time": times, lat_name: lats, "lon": lons},
    )


@pytest.mark.parametrize("lat_name", ["lat", "latitude"])
def test_get_lat_name_accepts_both_spellings(lat_name):
    """`get_lat_name` finds the latitude coordinate under either name."""
    ds = _grid(lat_name=lat_name)

    assert get_lat_name(ds) == lat_name


def test_get_lat_name_raises_without_latitude():
    """A dataset with no recognizable latitude coordinate raises `RuntimeError`."""
    ds = xr.Dataset({"tas": (("time",), [1.0, 2.0])}, coords={"time": [0, 1]})

    with pytest.raises(RuntimeError):
        get_lat_name(ds)


def test_global_mean_invariant(golden):
    """`global_mean` output is unchanged."""
    ds = _grid()
    out = global_mean(ds)

    frame = out["tas"].to_dataframe().reset_index()
    frame["time"] = frame["time"].astype(str)
    golden.assert_frame(frame, "data/global_mean", tolerance="value")


def test_global_mean_reduces_to_time_only():
    """`global_mean` collapses every dimension except time."""
    ds = _grid()
    out = global_mean(ds)

    assert set(out["tas"].dims) == {"time"}


def test_global_mean_of_constant_field_is_that_constant():
    """Averaging a spatially uniform field returns the constant.

    An analytic check on the weighting: whatever the weights are, if they are
    correctly normalized then a constant field must average to itself. A bug in
    the normalization shows up here immediately.
    """
    ds = _grid()
    ds["tas"] = xr.full_like(ds["tas"], 42.0)

    out = global_mean(ds)

    np.testing.assert_allclose(out["tas"].values, 42.0, rtol=1e-12)


def test_global_mean_weights_by_latitude():
    """The weighted mean of cos(latitude) exceeds the unweighted mean.

    With a cos(lat) field, area weighting emphasizes the wide equatorial cells
    where the field is largest, so the weighted result must be strictly greater
    than a naive arithmetic mean over grid cells. This confirms weighting is
    actually applied rather than silently skipped.
    """
    ds = _grid()

    weighted = float(global_mean(ds)["tas"].isel(time=0))
    unweighted = float(ds["tas"].isel(time=0).mean())

    assert weighted > unweighted
