"""Performance benchmarks for the stitches package.

These are **not** correctness tests. They measure wall-clock time so that
performance work can be shown to help, and so that a refactor intended to be
neutral can be shown not to have made things slower.

Correctness during optimization is the job of ``tests/regression``: the two
suites are meant to be used together. A performance change is acceptable only
when the benchmarks improve *and* the golden-output suite still passes.

Running
-------

Benchmarks are excluded from the default ``pytest`` run. To use them::

    # Record a baseline, ideally on a tagged commit
    pytest benchmarks --benchmark-only --benchmark-save=baseline

    # Compare a change against that baseline and fail on a big regression
    pytest benchmarks --benchmark-only \\
        --benchmark-compare=baseline \\
        --benchmark-compare-fail=mean:25%

Interpretation
--------------

Run benchmarks on one fixed OS and Python version. Comparing numbers across
machines, or across matrix jobs, is meaningless -- shared CI runners vary by
well over the regression threshold from run to run. Treat any benchmark whose
mean is under roughly 10ms as indicative only.
"""
