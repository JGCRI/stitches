"""Golden-output regression tests for the matching functions.

Covers :mod:`stitches.fx_match`. All fixtures are the small committed CSVs under
``stitches/data/example``, so these tests run entirely offline and are cheap
enough to gate every pull request.
"""

import pandas as pd
import pytest

from stitches.fx_match import (
    drop_hist_false_duplicates,
    internal_dist,
    match_neighborhood,
    shuffle_function,
)

# Every test in this module is an output-invariance check; the CI regression job
# selects on this marker.
pytestmark = pytest.mark.regression


@pytest.fixture(scope="module")
def target(read_example):
    """Return the example target data."""
    return read_example("test-target_dat")


@pytest.fixture(scope="module")
def archive(read_example):
    """Return the example archive data."""
    return read_example("test-archive_dat")


# ---------------------------------------------------------------------------
# match_neighborhood
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "tol,dedup,name",
    [
        (0.0, True, "match/neighborhood_tol0_dedup"),
        (0.0, False, "match/neighborhood_tol0_nodedup"),
        (0.1, True, "match/neighborhood_tol0.1_dedup"),
        (0.1, False, "match/neighborhood_tol0.1_nodedup"),
        (0.5, True, "match/neighborhood_tol0.5_dedup"),
    ],
)
def test_match_neighborhood_invariant(golden, target, archive, tol, dedup, name):
    """`match_neighborhood` output is unchanged across the tolerance sweep.

    The tolerance sweep matters because ``tol`` controls how many archive points
    fall inside each target neighborhood, exercising both the single-match and
    many-match branches of the distance filtering.
    """
    out = match_neighborhood(target, archive, tol=tol, drop_hist_duplicates=dedup)
    golden.assert_frame(out, name, tolerance="distance")


def test_match_neighborhood_self_match_invariant(golden, target):
    """Matching the target against itself is unchanged.

    A self-match is the degenerate case where every distance should be zero, so
    it isolates the distance computation from the neighborhood selection.
    """
    out = match_neighborhood(target, target, tol=0)
    golden.assert_frame(out, "match/neighborhood_self", tolerance="distance")


def test_match_neighborhood_self_match_distances_are_zero(target):
    """Every distance in a self-match is exactly zero.

    This is an absolute property rather than a recorded value, so it is asserted
    directly instead of against a golden file.
    """
    out = match_neighborhood(target, target, tol=0)

    for column in ("dist_dx", "dist_fx", "dist_l2"):
        assert (out[column] == 0).all(), f"{column} is not identically zero"


def test_match_neighborhood_row_count_is_stable(golden, target, archive):
    """Row counts per tolerance are unchanged.

    Recorded separately from the frames so that a change in *how many* matches
    are produced is reported as its own clearly-named failure.
    """
    counts = {
        str(tol): int(len(match_neighborhood(target, archive, tol=tol)))
        for tol in (0.0, 0.05, 0.1, 0.2, 0.5)
    }
    golden.assert_values(counts, "match/neighborhood_row_counts")


# ---------------------------------------------------------------------------
# internal_dist
# ---------------------------------------------------------------------------


def test_internal_dist_invariant(golden, target):
    """`internal_dist` output for a fixed probe point is unchanged."""
    out = internal_dist(target.fx[0], target.dx[0], target)
    golden.assert_frame(out, "match/internal_dist", tolerance="distance")


def test_internal_dist_with_tolerance_invariant(golden, target):
    """`internal_dist` with a nonzero tolerance is unchanged."""
    out = internal_dist(target.fx[0], target.dx[0], target, tol=0.1)
    golden.assert_frame(out, "match/internal_dist_tol0.1", tolerance="distance")


# ---------------------------------------------------------------------------
# drop_hist_false_duplicates
# ---------------------------------------------------------------------------


def test_drop_hist_false_duplicates_invariant(golden, read_example):
    """`drop_hist_false_duplicates` output is unchanged.

    This function resolves ties by keeping the row with the minimum ``idvalue``,
    which is order-sensitive, so pinning it guards against an incidental change
    in groupby iteration order silently changing which duplicate survives.
    """
    match_data = read_example("test-match_w_dup")
    out = drop_hist_false_duplicates(match_data)
    golden.assert_frame(out, "match/drop_hist_false_duplicates", tolerance="distance")


def test_drop_hist_false_duplicates_leaves_one_historical_experiment(read_example):
    """Only one historical target experiment survives deduplication.

    An invariant of the function's purpose: identical historical data pasted into
    every future experiment must collapse to a single representative.
    """
    match_data = read_example("test-match_w_dup")
    cleaned = drop_hist_false_duplicates(match_data)

    hist = cleaned[cleaned["target_start_yr"] <= 2020]["target_experiment"].unique()
    assert len(hist) == 1


# ---------------------------------------------------------------------------
# shuffle_function
# ---------------------------------------------------------------------------


def test_shuffle_function_seeded_invariant(golden, target):
    """A seeded shuffle produces an unchanged permutation.

    Pins the permutation so that a future change to how the seed is threaded
    through (for example, moving to a single shared generator) is caught rather
    than silently altering user-visible reproducible output.
    """
    out = shuffle_function(target, seed=20240101)
    golden.assert_frame(out, "match/shuffle_seed20240101", tolerance="value")


def test_shuffle_function_preserves_contents(target):
    """Shuffling is a pure permutation: no rows are added, dropped, or altered."""
    out = shuffle_function(target, seed=7)

    pd.testing.assert_frame_equal(
        out.sort_values(list(out.columns)).reset_index(drop=True),
        target.sort_values(list(target.columns)).reset_index(drop=True),
    )
