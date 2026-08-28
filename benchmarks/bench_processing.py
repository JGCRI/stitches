"""Benchmarks for the time-series processing functions.

`calculate_rolling_mean` and `chunk_ts` run over the entire CMIP6 archive during
package-data generation, which the documentation notes takes several hours. They
are therefore the functions where a constant-factor improvement translates into
the largest absolute saving.
"""

import pytest

from stitches.fx_processing import (
    calculate_rolling_mean,
    chunk_ts,
    get_chunk_info,
    subset_archive,
)

from .conftest import make_series


@pytest.mark.parametrize("size", [3, 9, 21])
def test_bench_calculate_rolling_mean(benchmark, multi_member_series, size):
    """Benchmark `calculate_rolling_mean` across window sizes.

    The implementation wraps a lambda in ``groupby.transform``; a wider window
    should not change the cost much, so a strong dependence on ``size`` would
    itself be a finding.
    """
    result = benchmark(calculate_rolling_mean, multi_member_series.copy(), size)

    assert len(result) == len(multi_member_series)


@pytest.mark.parametrize("n_groups", [4, 20, 100])
def test_bench_calculate_rolling_mean_scaling(benchmark, n_groups):
    """Benchmark `calculate_rolling_mean` as the number of groups grows.

    Group count, not row count, is what drives the grouped-transform overhead.
    """
    import pandas as pd

    frames = [
        make_series(experiment=f"ssp{i:03d}", ensemble=f"r{i}i1p1f1")
        for i in range(n_groups)
    ]
    data = pd.concat(frames).reset_index(drop=True)

    result = benchmark(calculate_rolling_mean, data.copy(), 9)

    assert len(result) == len(data)


@pytest.mark.parametrize("n", [5, 9, 20])
def test_bench_chunk_ts(benchmark, long_series, n):
    """Benchmark `chunk_ts` across chunk sizes."""
    result = benchmark(chunk_ts, long_series.copy(), n)

    assert "chunk" in result.columns


def test_bench_get_chunk_info(benchmark, long_series):
    """Benchmark `get_chunk_info`.

    Fits a scikit-learn ``LinearRegression`` per chunk inside a Python loop and
    grows the result with repeated ``pd.concat``, so it is a prime candidate for
    both a closed-form slope and a single terminal concat.
    """
    chunked = chunk_ts(long_series.copy(), 9)

    result = benchmark(get_chunk_info, chunked)

    assert len(result) > 0


def test_bench_subset_archive(benchmark, long_series):
    """Benchmark `subset_archive`."""
    info = get_chunk_info(chunk_ts(long_series.copy(), 9))
    end_years = sorted(info["end_yr"].unique())[:10]

    result = benchmark(subset_archive, info, end_years)

    assert len(result) <= len(info)
