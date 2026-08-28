"""Benchmarks for the matching functions.

`match_neighborhood` is the primary optimization target identified in the
development plan: it loops in Python over each target window group and calls
`internal_dist` per group, then concatenates. The parametrized archive sizes here
exist to reveal how that cost scales, which is what justifies (or fails to
justify) vectorizing it.
"""

import pytest

from stitches.fx_match import (
    drop_hist_false_duplicates,
    internal_dist,
    match_neighborhood,
    shuffle_function,
)

from .conftest import make_archive, make_chunk_frame


@pytest.mark.parametrize("tol", [0.0, 0.1, 0.5])
def test_bench_match_neighborhood_example(benchmark, example_target, example_archive, tol):
    """Benchmark `match_neighborhood` on the committed example data."""
    result = benchmark(match_neighborhood, example_target, example_archive, tol=tol)

    assert len(result) > 0


@pytest.mark.parametrize("n_windows", [28, 112, 280])
def test_bench_match_neighborhood_scaling(benchmark, n_windows):
    """Benchmark `match_neighborhood` as the number of target windows grows.

    The window counts correspond to roughly 1x, 4x, and 10x a single CMIP6
    trajectory. Comparing the three reveals whether the per-group Python loop
    scales linearly or worse.
    """
    target = make_chunk_frame(n_windows)
    archive = make_archive(n_windows, n_members=4)

    result = benchmark(match_neighborhood, target, archive, tol=0.1)

    assert len(result) > 0


@pytest.mark.parametrize("n_members", [2, 8, 16])
def test_bench_match_neighborhood_archive_width(benchmark, n_members):
    """Benchmark `match_neighborhood` as the archive gains ensemble members.

    Archive size grows independently of target size, so this isolates the cost of
    searching a wider archive from the cost of iterating more target windows.
    """
    target = make_chunk_frame(28)
    archive = make_archive(28, n_members=n_members)

    result = benchmark(match_neighborhood, target, archive, tol=0.1)

    assert len(result) > 0


def test_bench_internal_dist(benchmark, example_target):
    """Benchmark `internal_dist`, the innermost distance computation."""
    result = benchmark(
        internal_dist, example_target.fx[0], example_target.dx[0], example_target
    )

    assert len(result) > 0


def test_bench_drop_hist_false_duplicates(benchmark, example_match_with_duplicates):
    """Benchmark `drop_hist_false_duplicates`.

    Another per-group loop with a terminal `pd.concat`, and a candidate for the
    same vectorization treatment as `match_neighborhood`.
    """
    result = benchmark(
        drop_hist_false_duplicates, example_match_with_duplicates.copy()
    )

    assert len(result) > 0


def test_bench_shuffle_function(benchmark, multi_member_series):
    """Benchmark `shuffle_function` on a large frame."""
    result = benchmark(shuffle_function, multi_member_series, seed=1)

    assert len(result) == len(multi_member_series)
