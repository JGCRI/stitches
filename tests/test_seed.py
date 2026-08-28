"""Tests for the ``seed`` arguments on the randomized entry points.

`shuffle_function` and `permute_stitching_recipes` both draw random samples.
Before the ``seed`` parameter existed, the only way to obtain a reproducible
recipe was ``permute_stitching_recipes(testing=True)``, which hardcoded
``random_state=1``. Overloading a "testing" flag to mean "be deterministic" is
awkward for users who legitimately need reproducible output, so an explicit
``seed`` was added.

These tests pin the backward-compatibility contract:

* omitting ``seed`` reproduces the historical nondeterministic behavior;
* ``testing=True`` remains exactly equivalent to ``seed=1``;
* distinct seeds produce distinct draws, i.e. the seed is actually plumbed
  through rather than silently ignored.
"""

import pandas as pd
import pytest

from stitches.fx_match import match_neighborhood, shuffle_function
from stitches.fx_recipe import permute_stitching_recipes

from test_fx_recipe import TestRecipe

TARGET_DATA = TestRecipe.TARGET_DATA
ARCHIVE_DATA = TestRecipe.ARCHIVE_DATA


@pytest.fixture(scope="module")
def matched():
    """Return matched data shared by the permutation tests."""
    return match_neighborhood(TARGET_DATA, ARCHIVE_DATA, tol=0.07)


def _permute(matched_data, **kwargs):
    """Call `permute_stitching_recipes` with the shared test arguments."""
    return permute_stitching_recipes(
        N_matches=2, matched_data=matched_data, archive=ARCHIVE_DATA, **kwargs
    )


# ---------------------------------------------------------------------------
# shuffle_function
# ---------------------------------------------------------------------------


def test_shuffle_function_preserves_shape():
    """Shuffling reorders rows without adding or dropping any."""
    subset = TARGET_DATA.head(10)
    out = shuffle_function(subset)

    assert subset.shape == out.shape
    assert sorted(out["year"].tolist()) == sorted(subset["year"].tolist())


def test_shuffle_function_seed_is_reproducible():
    """The same seed yields the same permutation."""
    first = shuffle_function(TARGET_DATA, seed=42)
    second = shuffle_function(TARGET_DATA, seed=42)

    pd.testing.assert_frame_equal(first, second)


def test_shuffle_function_distinct_seeds_differ():
    """Different seeds yield different permutations."""
    first = shuffle_function(TARGET_DATA, seed=42)
    second = shuffle_function(TARGET_DATA, seed=43)

    assert not first.equals(second)


def test_shuffle_function_without_seed_is_random():
    """Omitting the seed preserves the historical nondeterministic behavior.

    Repeated draws are compared rather than a single pair to keep the odds of a
    coincidental match negligible for a 28-row frame.
    """
    draws = [shuffle_function(TARGET_DATA) for _ in range(5)]

    assert any(not draws[0].equals(other) for other in draws[1:])


# ---------------------------------------------------------------------------
# permute_stitching_recipes
# ---------------------------------------------------------------------------


def test_permute_seed_matches_legacy_testing_flag(matched):
    """``seed=1`` must reproduce ``testing=True`` exactly.

    This is the backward-compatibility guarantee: published results generated
    with ``testing=True`` remain reproducible via the new parameter, so the seed
    refactor cannot have altered any existing output.
    """
    legacy = _permute(matched, testing=True)
    seeded = _permute(matched, seed=1)

    pd.testing.assert_frame_equal(legacy, seeded)


def test_permute_testing_flag_is_still_reproducible(matched):
    """``testing=True`` remains deterministic across calls."""
    first = _permute(matched, testing=True)
    second = _permute(matched, testing=True)

    pd.testing.assert_frame_equal(first, second)


def test_permute_seed_is_reproducible(matched):
    """A given seed yields the same recipe across calls."""
    first = _permute(matched, seed=1234)
    second = _permute(matched, seed=1234)

    pd.testing.assert_frame_equal(first, second)


def test_permute_distinct_seeds_differ(matched):
    """Different seeds select different archive points, proving the seed is used."""
    first = _permute(matched, seed=1)
    second = _permute(matched, seed=99)

    assert not first.equals(second)


def test_permute_without_seed_is_random(matched):
    """Omitting both ``seed`` and ``testing`` preserves nondeterminism."""
    draws = [_permute(matched) for _ in range(5)]

    assert any(not draws[0].equals(other) for other in draws[1:])


def test_permute_seed_overrides_testing_flag(matched):
    """An explicit ``seed`` takes precedence over ``testing=True``."""
    with_seed = _permute(matched, testing=True, seed=99)
    seed_only = _permute(matched, seed=99)
    legacy = _permute(matched, testing=True)

    pd.testing.assert_frame_equal(with_seed, seed_only)
    assert not with_seed.equals(legacy)
