"""Golden-output regression tests for the stitches package.

The purpose of this package is to guarantee **output invariance**: refactors,
dependency upgrades, and performance work must not change the numbers that
stitches produces, except where the current output is provably wrong.

How it works
------------

Each test calls a function and compares the result against a *golden* artifact
recorded from a known-good baseline. Artifacts live in ``golden/`` as Parquet
files (stable, typed, compact -- unlike CSV, which invites float-formatting
drift that can both mask real diffs and manufacture fake ones).

Regenerating goldens
--------------------

Golden files are regenerated only deliberately::

    pytest tests/regression --update-golden

Policy: any PR that changes a file under ``tests/regression/golden/`` **must**
include a ``CHANGELOG.md`` entry under ``### Changed -- outputs`` explaining
which defect the new output corrects, and must be reviewed by a domain
maintainer rather than only a code reviewer. An unexplained golden update
defeats the entire purpose of this suite.
"""
