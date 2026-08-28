"""Benchmarks for recipe construction.

`permute_stitching_recipes` is the most expensive pure-Python routine in the
package. It runs a while loop that repeatedly samples candidate recipes and
rejects those violating the duplicate and collapse constraints, so its cost
depends on how often it has to retry -- which in turn depends on the matching
tolerance and on how many realizations are requested.

All calls pass an explicit ``seed`` so the amount of work done is identical from
run to run. Without that, retry counts would vary between runs and the timings
would be too noisy to compare.
"""

import pytest

from stitches.fx_match import match_neighborhood
from stitches.fx_recipe import (
    get_num_perms,
    handle_final_period,
    handle_transition_periods,
    permute_stitching_recipes,
    remove_duplicates,
)

from .conftest import make_archive, make_chunk_frame


@pytest.fixture(scope="module")
def scenario():
    """Return a target/archive pair large enough to be worth timing."""
    target = make_chunk_frame(28)
    archive = make_archive(28, n_members=6)
    return target, archive


@pytest.fixture(scope="module")
def matched(scenario):
    """Return matched data for the benchmark scenario."""
    target, archive = scenario
    return match_neighborhood(target, archive, tol=0.1)


def test_bench_get_num_perms(benchmark, matched):
    """Benchmark `get_num_perms`, several chained groupby aggregations."""
    result = benchmark(get_num_perms, matched)

    assert len(result) == 2


def test_bench_remove_duplicates(benchmark, scenario):
    """Benchmark `remove_duplicates` on nearest-neighbor matches."""
    target, archive = scenario
    nn = match_neighborhood(target, archive, tol=0.0)

    result = benchmark(remove_duplicates, md=nn.copy(), archive=archive)

    assert len(result) > 0


@pytest.mark.parametrize("n_matches", [1, 2, 5])
def test_bench_permute_stitching_recipes(benchmark, matched, scenario, n_matches):
    """Benchmark `permute_stitching_recipes` as more realizations are requested.

    Cost is expected to grow faster than linearly in ``N_matches``, because each
    additional realization must avoid every archive point already consumed by the
    previous ones, raising the rejection rate.
    """
    _, archive = scenario

    result = benchmark(
        permute_stitching_recipes,
        N_matches=n_matches,
        matched_data=matched,
        archive=archive,
        seed=1,
    )

    assert len(result) > 0


@pytest.mark.parametrize("tol", [0.05, 0.1, 0.3])
def test_bench_permute_by_tolerance(benchmark, scenario, tol):
    """Benchmark `permute_stitching_recipes` across matching tolerances.

    A wider tolerance means more candidates per window: more choice, but also more
    matched rows to carry through the constraint checks.
    """
    target, archive = scenario
    matched_data = match_neighborhood(target, archive, tol=tol)

    result = benchmark(
        permute_stitching_recipes,
        N_matches=2,
        matched_data=matched_data,
        archive=archive,
        seed=1,
    )

    assert len(result) > 0


def test_bench_handle_transition_periods(benchmark, matched, scenario):
    """Benchmark `handle_transition_periods`."""
    _, archive = scenario
    messy = permute_stitching_recipes(
        N_matches=1, matched_data=matched, archive=archive, seed=1
    )

    result = benchmark(handle_transition_periods, messy.copy())

    assert len(result) > 0


def test_bench_handle_final_period(benchmark, matched, scenario):
    """Benchmark `handle_final_period`."""
    _, archive = scenario
    messy = handle_transition_periods(
        permute_stitching_recipes(
            N_matches=1, matched_data=matched, archive=archive, seed=1
        )
    )

    result = benchmark(handle_final_period, messy.copy())

    assert len(result) > 0
