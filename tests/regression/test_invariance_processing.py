"""Golden-output regression tests for the time-series processing functions.

Covers :mod:`stitches.fx_processing`, which performs the smoothing and chunking
that produce the ``fx`` (level) and ``dx`` (rate of change) values the matching
algorithm depends on. An error here silently propagates into every recipe, so
these are among the most valuable invariants in the suite.

The fixtures are synthetic but deterministic: a smooth warming trend plus a fixed
oscillation, built from a seeded generator so the series is byte-identical on
every platform. Synthetic data is preferable here because it lets the window
arithmetic be exercised at exact, known lengths.
"""

import numpy as np
import pandas as pd
import pytest

from stitches.fx_processing import (
    calculate_rolling_mean,
    chunk_ts,
    get_chunk_info,
    subset_archive,
)

# Every test in this module is an output-invariance check; the CI regression job
# selects on this marker.
pytestmark = pytest.mark.regression


def _series(start=1850, end=2100, model="test_model", experiment="ssp245", ensemble="r1i1p1f1"):
    """Build a deterministic synthetic annual temperature series.

    The signal combines a quadratic warming trend, a decadal oscillation, and a
    small seeded pseudo-random component. The seeded component matters: it
    prevents the rolling mean and the linear fit from operating on a perfectly
    smooth curve, where a bug in window centering could go unnoticed.

    :return: A DataFrame with the columns the processing functions require.
    :rtype: pandas.DataFrame
    """
    years = np.arange(start, end + 1)
    n = len(years)

    # Fixed generator seed keeps this reproducible across platforms and versions.
    rng = np.random.default_rng(20240101)
    trend = 0.00012 * (years - start) ** 2
    oscillation = 0.35 * np.sin(np.arange(n) / 11.0)
    noise = rng.normal(0.0, 0.05, size=n)

    return pd.DataFrame(
        {
            "year": years,
            "value": trend + oscillation + noise,
            "variable": "tas",
            "model": model,
            "experiment": experiment,
            "ensemble": ensemble,
            "unit": "K",
        }
    )


@pytest.fixture(scope="module")
def series():
    """Return the synthetic single-realization series."""
    return _series()


@pytest.fixture(scope="module")
def multi_series():
    """Return a multi-realization, multi-experiment series.

    Needed because `calculate_rolling_mean` groups by
    model/experiment/ensemble/variable; a single group would not exercise the
    grouping at all.
    """
    frames = [
        _series(experiment=exp, ensemble=ens)
        for exp in ("historical", "ssp245", "ssp585")
        for ens in ("r1i1p1f1", "r2i1p1f1")
    ]
    return pd.concat(frames).reset_index(drop=True)


# ---------------------------------------------------------------------------
# calculate_rolling_mean
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("size", [3, 9, 11])
def test_rolling_mean_invariant(golden, multi_series, size):
    """`calculate_rolling_mean` output is unchanged across window sizes.

    Window size 9 is the package default; 3 and 11 bracket it so an off-by-one in
    the window arithmetic shows up as a shape or edge-value change.
    """
    out = calculate_rolling_mean(multi_series.copy(), size)
    golden.assert_frame(out, f"processing/rolling_mean_w{size}", tolerance="value")


def test_rolling_mean_preserves_row_count(multi_series):
    """Smoothing returns one row per input row.

    The implementation uses ``min_periods=1`` specifically so that the first and
    last ``(size-1)/2`` years are retained rather than becoming NaN. This asserts
    that intent directly.
    """
    out = calculate_rolling_mean(multi_series.copy(), 9)

    assert len(out) == len(multi_series)
    assert out["value"].notna().all()


def test_rolling_mean_edges_are_not_nan(multi_series):
    """The window edges carry real values, not NaN."""
    out = calculate_rolling_mean(multi_series.copy(), 9)
    column = "rollingAvg" if "rollingAvg" in out.columns else "value"

    assert out[column].notna().all(), f"{column} contains NaN at the series edges"


# ---------------------------------------------------------------------------
# chunk_ts
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("n", [5, 9, 20])
def test_chunk_ts_invariant(golden, series, n):
    """`chunk_ts` chunk assignment is unchanged across window sizes."""
    out = chunk_ts(series.copy(), n)
    golden.assert_frame(out, f"processing/chunk_ts_n{n}", tolerance="value")


@pytest.mark.parametrize("base_chunk", [0, 1, 4])
def test_chunk_ts_staggered_invariant(golden, series, base_chunk):
    """`chunk_ts` staggered offsets are unchanged.

    ``base_chunk`` drops leading years to build staggered archives. Pinning
    several offsets guards the slicing arithmetic.
    """
    out = chunk_ts(series.copy(), 9, base_chunk=base_chunk)
    golden.assert_frame(out, f"processing/chunk_ts_n9_base{base_chunk}", tolerance="value")


def test_chunk_ts_chunk_sizes(series):
    """Every chunk holds ``n`` years except a possibly short final chunk."""
    n = 9
    out = chunk_ts(series.copy(), n)
    sizes = out.groupby("chunk").size()

    assert (sizes.iloc[:-1] == n).all(), "an interior chunk is not n years long"
    assert 0 < sizes.iloc[-1] <= n, "final chunk size out of range"


def test_chunk_ts_rejects_base_chunk_larger_than_n(series):
    """``base_chunk > n`` is rejected."""
    with pytest.raises(TypeError):
        chunk_ts(series.copy(), 5, base_chunk=6)


def test_chunk_ts_rejects_multiple_variables(series):
    """A frame holding more than one variable is rejected."""
    mixed = series.copy()
    mixed.loc[mixed.index[:10], "variable"] = "pr"

    with pytest.raises(TypeError):
        chunk_ts(mixed, 9)


# ---------------------------------------------------------------------------
# get_chunk_info
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("n", [5, 9])
def test_get_chunk_info_invariant(golden, series, n):
    """`get_chunk_info` fx and dx values are unchanged.

    ``dx`` comes from a scikit-learn ``LinearRegression`` fit per chunk, so this
    test also pins the package against a change in that dependency's numerics.
    """
    chunked = chunk_ts(series.copy(), n)
    out = get_chunk_info(chunked)
    golden.assert_frame(out, f"processing/chunk_info_n{n}", tolerance="value")


def test_get_chunk_info_one_row_per_chunk(series):
    """`get_chunk_info` summarizes each chunk to exactly one row."""
    chunked = chunk_ts(series.copy(), 9)
    out = get_chunk_info(chunked)

    assert len(out) == chunked["chunk"].nunique()


def test_get_chunk_info_years_are_integers(series):
    """Year columns are returned as integers, not floats or categoricals."""
    chunked = chunk_ts(series.copy(), 9)
    out = get_chunk_info(chunked)

    for column in ("start_yr", "end_yr", "year"):
        assert pd.api.types.is_integer_dtype(out[column]), f"{column} is not integral"


def test_get_chunk_info_year_bounds_are_consistent(series):
    """Each chunk's representative year lies within its own start/end bounds."""
    chunked = chunk_ts(series.copy(), 9)
    out = get_chunk_info(chunked)

    assert (out["start_yr"] <= out["year"]).all()
    assert (out["year"] <= out["end_yr"]).all()


# ---------------------------------------------------------------------------
# subset_archive
# ---------------------------------------------------------------------------


def test_subset_archive_invariant(golden, series):
    """`subset_archive` selection is unchanged."""
    staggered = pd.concat(
        [chunk_ts(series.copy(), 9, base_chunk=offset) for offset in range(0, 3)]
    ).reset_index(drop=True)
    info = get_chunk_info(
        chunk_ts(series.copy(), 9)
    )
    end_years = sorted(info["end_yr"].unique())[:5]

    archive = info.copy()
    out = subset_archive(archive, end_years)
    golden.assert_frame(out, "processing/subset_archive", tolerance="value")

    assert set(out["end_yr"]).issubset(set(end_years))
    assert len(staggered) > 0  # staggered construction must not silently produce nothing
