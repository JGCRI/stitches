"""Golden-output regression tests for the recipe construction functions.

Covers :mod:`stitches.fx_recipe`, the most algorithmically involved part of the
package. `permute_stitching_recipes` samples archive points per target window
while enforcing several non-local constraints (no duplicate archive points, no
envelope collapse across generated realizations), and the transition handlers
then rewrite window boundaries. Small changes here are easy to make by accident
and hard to notice, which is exactly what these goldens are for.

All randomized calls pass an explicit ``seed`` so the recorded artifacts are
reproducible. Tests that must not depend on a particular draw assert structural
properties instead of recorded values.
"""

import pandas as pd
import pytest

from stitches.fx_match import match_neighborhood
from stitches.fx_recipe import (
    get_num_perms,
    handle_final_period,
    handle_transition_periods,
    permute_stitching_recipes,
    remove_duplicates,
)

# The synthetic target/archive pair used by the existing unit tests. Reused here
# so the regression suite and the unit suite describe the same scenario.
from tests.test_fx_recipe import TestRecipe

TARGET_DATA = TestRecipe.TARGET_DATA
ARCHIVE_DATA = TestRecipe.ARCHIVE_DATA


@pytest.fixture(scope="module")
def matched():
    """Return matched data at the tolerance used by the existing unit tests."""
    return match_neighborhood(TARGET_DATA, ARCHIVE_DATA, tol=0.07)


@pytest.fixture(scope="module")
def matched_wide():
    """Return matched data at a wider tolerance, giving more candidates per window."""
    return match_neighborhood(TARGET_DATA, ARCHIVE_DATA, tol=0.2)


@pytest.fixture(scope="module")
def matched_nn():
    """Return nearest-neighbor matches: exactly one archive point per target year.

    ``remove_duplicates`` requires singular matches and raises `TypeError`
    otherwise, so it must be fed ``tol=0`` output rather than a permuted recipe.
    """
    return match_neighborhood(TARGET_DATA, ARCHIVE_DATA, tol=0.0)


# ---------------------------------------------------------------------------
# get_num_perms
# ---------------------------------------------------------------------------


def test_get_num_perms_targets_invariant(golden, matched):
    """The per-target permutation summary is unchanged.

    ``get_num_perms`` decides how many collapse-free realizations each target can
    support, which in turn drives the order targets are processed in. A change
    here reorders the whole construction.
    """
    targets, _ = get_num_perms(matched)
    golden.assert_frame(targets, "recipe/num_perms_targets", tolerance="exact")


def test_get_num_perms_guide_invariant(golden, matched):
    """The per-window permutation guide is unchanged."""
    _, guide = get_num_perms(matched)
    golden.assert_frame(guide, "recipe/num_perms_guide", tolerance="exact")


def test_get_num_perms_counts_are_positive(matched):
    """Every target window has at least one candidate match."""
    _, guide = get_num_perms(matched)
    count_column = "n_matches" if "n_matches" in guide.columns else guide.columns[-1]

    assert (guide[count_column] > 0).all()


# ---------------------------------------------------------------------------
# remove_duplicates
# ---------------------------------------------------------------------------


def test_remove_duplicates_invariant(golden, matched_nn):
    """`remove_duplicates` output is unchanged.

    Where two target years claim the same archive point, the closer one keeps it
    and the other is re-matched against an archive with the taken points removed.
    This pins which target year wins and what the loser is re-matched to.
    """
    out = remove_duplicates(md=matched_nn.copy(), archive=ARCHIVE_DATA)
    golden.assert_frame(out, "recipe/remove_duplicates", tolerance="distance")


def test_remove_duplicates_leaves_no_repeated_archive_point(matched_nn):
    """No archive point is used twice after deduplication."""
    out = remove_duplicates(md=matched_nn.copy(), archive=ARCHIVE_DATA)
    archive_cols = [c for c in out.columns if c.startswith("archive_")]

    assert not out[archive_cols].duplicated().any()


def test_remove_duplicates_preserves_target_coverage(matched_nn):
    """Re-matching never drops a target year."""
    out = remove_duplicates(md=matched_nn.copy(), archive=ARCHIVE_DATA)

    assert set(out["target_year"]) == set(matched_nn["target_year"])


def test_remove_duplicates_rejects_multiple_matches_per_year(matched):
    """Feeding non-singular matches raises `TypeError`.

    Documents a real precondition of the function: it operates on one recipe at a
    time, not on the full many-to-many match table.
    """
    with pytest.raises(TypeError):
        remove_duplicates(md=matched.copy(), archive=ARCHIVE_DATA)


# ---------------------------------------------------------------------------
# permute_stitching_recipes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("n_matches", [1, 2])
def test_permute_stitching_recipes_invariant(golden, matched, n_matches):
    """Seeded recipe permutations are unchanged."""
    out = permute_stitching_recipes(
        N_matches=n_matches, matched_data=matched, archive=ARCHIVE_DATA, seed=1
    )
    golden.assert_frame(out, f"recipe/permute_N{n_matches}_seed1", tolerance="distance")


def test_permute_stitching_recipes_wide_tolerance_invariant(golden, matched_wide):
    """Seeded permutations at a wider tolerance are unchanged.

    A wider tolerance means more candidates per window, exercising the sampling
    and duplicate-rejection loop far more heavily than the narrow case.
    """
    out = permute_stitching_recipes(
        N_matches=2, matched_data=matched_wide, archive=ARCHIVE_DATA, seed=1
    )
    golden.assert_frame(out, "recipe/permute_N2_tol0.2_seed1", tolerance="distance")


def test_permute_covers_every_target_window(matched):
    """A generated recipe spans exactly the target's time windows."""
    out = permute_stitching_recipes(
        N_matches=1, matched_data=matched, archive=ARCHIVE_DATA, seed=1
    )

    assert set(out["target_year"]) == set(TARGET_DATA["year"])


def test_permute_draws_only_from_the_archive(matched):
    """Every selected window really exists in the archive."""
    out = permute_stitching_recipes(
        N_matches=2, matched_data=matched, archive=ARCHIVE_DATA, seed=1
    )

    archive_cols = [c for c in out.columns if c.startswith("archive_")]
    check = out[archive_cols].copy()
    check.columns = [c.replace("archive_", "") for c in check.columns]

    assert len(check.merge(ARCHIVE_DATA)) == len(check), (
        "recipe contains a window that is not present in the archive"
    )


def test_permute_uses_each_archive_point_once(matched):
    """No archive point is reused within a generated realization."""
    out = permute_stitching_recipes(
        N_matches=2, matched_data=matched, archive=ARCHIVE_DATA, seed=1
    )

    archive_cols = [c for c in out.columns if c.startswith("archive_")]
    for _, group in out.groupby("stitching_id"):
        assert not group[archive_cols].duplicated().any(), (
            "an archive point is reused within one stitching_id"
        )


def test_permute_has_no_envelope_collapse(matched_wide):
    """Distinct target realizations do not share an archive point in the same year.

    "Envelope collapse" is the failure mode where independently generated
    realizations converge onto identical archive data, understating the spread of
    the emulated ensemble. Guarding it is a core correctness property.
    """
    target2 = pd.concat([ARCHIVE_DATA, TARGET_DATA]).reset_index(drop=True).copy()
    archive2 = target2.copy()

    out = permute_stitching_recipes(
        N_matches=2,
        matched_data=match_neighborhood(target2, archive2, tol=0.2),
        archive=archive2,
        seed=1,
    )

    archive_cols = [c for c in out.columns if c.startswith("archive_")]
    for year in out["target_year"].unique():
        window = out[out["target_year"] == year]
        assert len(window) == len(window[archive_cols].drop_duplicates()), (
            f"envelope collapse at target_year={year}"
        )


def test_permute_rejects_multiple_target_experiments(matched):
    """Mixing target experiments in one call is rejected."""
    mixed = matched.copy()
    mixed.loc[mixed.index[: len(mixed) // 2], "target_experiment"] = "ssp585"

    with pytest.raises(TypeError):
        permute_stitching_recipes(
            N_matches=1, matched_data=mixed, archive=ARCHIVE_DATA, seed=1
        )


# ---------------------------------------------------------------------------
# transition handling
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def messy_recipe(matched):
    """Return a seeded unformatted recipe for the transition handlers."""
    return permute_stitching_recipes(
        N_matches=1, matched_data=matched, archive=ARCHIVE_DATA, seed=1
    )


def test_handle_transition_periods_invariant(golden, messy_recipe):
    """`handle_transition_periods` output is unchanged.

    This function splits windows that straddle the historical/future boundary, so
    an error produces recipes that request data from the wrong experiment.
    """
    out = handle_transition_periods(messy_recipe.copy())
    golden.assert_frame(out, "recipe/handle_transition_periods", tolerance="distance")


def test_handle_final_period_invariant(golden, messy_recipe):
    """`handle_final_period` output is unchanged."""
    out = handle_final_period(handle_transition_periods(messy_recipe.copy()))
    golden.assert_frame(out, "recipe/handle_final_period", tolerance="distance")


def test_transition_handling_preserves_target_coverage(messy_recipe):
    """Transition handling never leaves a gap in the target timeline.

    The union of target windows must still cover the original span contiguously;
    a gap would silently produce a stitched series with missing years.
    """
    out = handle_final_period(handle_transition_periods(messy_recipe.copy()))

    for _, group in out.groupby("stitching_id"):
        ordered = group.sort_values("target_start_yr")
        starts = ordered["target_start_yr"].tolist()
        ends = ordered["target_end_yr"].tolist()

        for previous_end, next_start in zip(ends, starts[1:]):
            assert next_start == previous_end + 1, (
                f"gap or overlap in target coverage: {previous_end} -> {next_start}"
            )
