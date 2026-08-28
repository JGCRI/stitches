"""Comparison helpers and fixtures for the golden-output regression suite.

The central helper is :func:`assert_matches_golden`, which compares a DataFrame
against a recorded Parquet artifact.

Canonicalization
----------------

Before comparison both frames are passed through :func:`canonicalize`, which
sorts columns by name and rows by their full contents. This deliberately makes
the comparison insensitive to row and column *ordering* while remaining fully
sensitive to row and column *content*.

The rationale: ``groupby``/``concat``-heavy code legitimately changes iteration
order across pandas versions and across refactors that are otherwise pure. If
ordering were significant, the suite would emit false failures on every such
change and would quickly be ignored. Where ordering is genuinely part of the
contract -- for example, `make_recipe` sorts by ``stitching_id`` and
``target_start_yr`` before returning -- it is asserted explicitly by a dedicated
test rather than implicitly by the golden comparison.

Tolerances
----------

Integral and categorical data must match exactly. Floating point results are
compared with a tight but nonzero tolerance, because NumPy and pandas are free
to reassociate reductions between versions; demanding bit-equality there would
produce failures that carry no scientific meaning. See ``TOLERANCES``.
"""

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

#: Directory holding recorded golden artifacts.
GOLDEN_DIR = Path(__file__).parent / "golden"

#: Named tolerance presets, keyed by the kind of quantity being compared.
#:
#: ``exact``    - indices, years, labels, filenames, counts. Any change is a bug.
#: ``distance`` - the L2/dx/fx match distances; a few ULP of drift is acceptable.
#: ``value``    - temperature values derived from means over the archive.
#: ``field``    - gridded NetCDF field values, which accumulate more error.
TOLERANCES = {
    "exact": {"rtol": 0.0, "atol": 0.0},
    "distance": {"rtol": 1e-12, "atol": 0.0},
    "value": {"rtol": 1e-10, "atol": 0.0},
    "field": {"rtol": 1e-6, "atol": 1e-9},
}


def canonicalize(df):
    """Return ``df`` in a deterministic row and column order.

    Columns are sorted by name and rows are sorted by their full contents using
    a stable sort, so the result depends only on the *set* of rows and columns
    present, not on the order the producing code happened to emit them in.

    :param df: The DataFrame to canonicalize.
    :type df: pandas.DataFrame
    :return: A new DataFrame with sorted columns, sorted rows, and a reset index.
    :rtype: pandas.DataFrame
    """
    out = df.reindex(sorted(df.columns), axis=1)
    if len(out) > 0:
        out = out.sort_values(list(out.columns), kind="mergesort")
    return out.reset_index(drop=True)


def frame_digest(df):
    """Return a short, stable SHA-256 digest of a canonicalized DataFrame.

    Used for artifacts that are too large to vendor, where only a checksum is
    stored. Note that this is exact by construction and therefore cannot express
    a floating-point tolerance; reserve it for frames whose values are integral
    or categorical, or where any change at all is worth investigating.

    :param df: The DataFrame to digest.
    :type df: pandas.DataFrame
    :return: The first 32 hex characters of the digest.
    :rtype: str
    """
    canonical = canonicalize(df)
    hashed = pd.util.hash_pandas_object(canonical, index=False)
    return hashlib.sha256(hashed.values.tobytes()).hexdigest()[:32]


class GoldenComparer:
    """Compares frames against golden artifacts, or rewrites them on request.

    Instantiated by the :func:`golden` fixture. When ``update`` is True the
    comparison methods write the incoming data to disk and pass, which is how
    ``--update-golden`` regenerates the suite.
    """

    def __init__(self, update=False, golden_dir=GOLDEN_DIR):
        self.update = update
        self.golden_dir = Path(golden_dir)
        #: Relative paths written during this session, for reporting.
        self.written = []

    # -- frames ----------------------------------------------------------

    def assert_frame(self, actual, name, tolerance="distance"):
        """Assert ``actual`` matches the golden Parquet artifact ``name``.

        :param actual: The frame produced by the code under test.
        :type actual: pandas.DataFrame
        :param name: Artifact path relative to the golden directory, without
            the ``.parquet`` suffix, e.g. ``"match/tol0"``.
        :type name: str
        :param tolerance: Key into :data:`TOLERANCES`.
        :type tolerance: str
        """
        if tolerance not in TOLERANCES:
            raise ValueError(
                f"unknown tolerance {tolerance!r}; expected one of {sorted(TOLERANCES)}"
            )

        path = self.golden_dir / f"{name}.parquet"

        if self.update:
            path.parent.mkdir(parents=True, exist_ok=True)
            canonicalize(actual).to_parquet(path, index=False)
            self.written.append(str(path.relative_to(self.golden_dir)))
            return

        if not path.is_file():
            pytest.fail(
                f"missing golden artifact {path}.\n"
                f"Generate it with: pytest tests/regression --update-golden"
            )

        expected = pd.read_parquet(path)

        actual_c = canonicalize(actual)
        expected_c = canonicalize(expected)

        # Report a shape or column mismatch directly; assert_frame_equal's
        # message for these cases is much harder to read.
        if list(actual_c.columns) != list(expected_c.columns):
            raise AssertionError(
                f"{name}: column mismatch\n"
                f"  unexpected: {sorted(set(actual_c.columns) - set(expected_c.columns))}\n"
                f"  missing:    {sorted(set(expected_c.columns) - set(actual_c.columns))}"
            )
        if len(actual_c) != len(expected_c):
            raise AssertionError(
                f"{name}: row count changed: expected {len(expected_c)}, got {len(actual_c)}"
            )

        assert_frame_equal(
            actual_c,
            expected_c,
            # Integer width and categorical-vs-object differences vary across
            # platforms and pandas versions without any change in meaning.
            check_dtype=False,
            check_categorical=False,
            obj=f"golden[{name}]",
            **TOLERANCES[tolerance],
        )

    # -- digests ---------------------------------------------------------

    def assert_digest(self, actual, name):
        """Assert the digest of ``actual`` matches the recorded JSON digest.

        For artifacts too large to vendor as Parquet.

        :param actual: The frame produced by the code under test.
        :type actual: pandas.DataFrame
        :param name: Artifact path relative to the golden directory, without
            the ``.json`` suffix.
        :type name: str
        """
        path = self.golden_dir / f"{name}.json"
        digest = frame_digest(actual)
        record = {"digest": digest, "rows": int(len(actual)), "columns": sorted(actual.columns)}

        if self.update:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
            self.written.append(str(path.relative_to(self.golden_dir)))
            return

        if not path.is_file():
            pytest.fail(
                f"missing golden digest {path}.\n"
                f"Generate it with: pytest tests/regression --update-golden"
            )

        expected = json.loads(path.read_text())

        assert record["columns"] == expected["columns"], f"{name}: columns changed"
        assert record["rows"] == expected["rows"], (
            f"{name}: row count changed: expected {expected['rows']}, got {record['rows']}"
        )
        assert digest == expected["digest"], (
            f"{name}: content digest changed.\n"
            f"  expected {expected['digest']}\n"
            f"  actual   {digest}\n"
            f"If this change is intentional and corrects a defect, regenerate with "
            f"--update-golden and document it in CHANGELOG.md."
        )

    # -- scalars ---------------------------------------------------------

    def assert_values(self, actual, name):
        """Assert a JSON-serializable mapping matches the recorded values.

        Useful for pinning things that are not frames, such as generated
        filenames or summary statistics.

        :param actual: A JSON-serializable mapping.
        :type actual: dict
        :param name: Artifact path relative to the golden directory, without
            the ``.json`` suffix.
        :type name: str
        """
        path = self.golden_dir / f"{name}.json"

        if self.update:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(actual, indent=2, sort_keys=True) + "\n")
            self.written.append(str(path.relative_to(self.golden_dir)))
            return

        if not path.is_file():
            pytest.fail(
                f"missing golden values {path}.\n"
                f"Generate it with: pytest tests/regression --update-golden"
            )

        expected = json.loads(path.read_text())
        assert actual == expected, f"{name}: recorded values changed"


@pytest.fixture(scope="session")
def golden(request):
    """Return a :class:`GoldenComparer` honoring the ``--update-golden`` flag."""
    update = request.config.getoption("update_golden")
    comparer = GoldenComparer(update=update)

    if update:
        comparer.golden_dir.mkdir(parents=True, exist_ok=True)

    yield comparer

    if update and comparer.written:
        print(
            f"\n--update-golden rewrote {len(comparer.written)} artifact(s) under "
            f"{comparer.golden_dir}:\n  " + "\n  ".join(sorted(comparer.written))
            + "\n\nRemember to document intentional output changes in CHANGELOG.md."
        )


@pytest.fixture(autouse=True)
def _mark_regression(request):
    """Apply the ``regression`` marker to everything in this package.

    Keeps ``pytest -m regression`` accurate without requiring each module to
    repeat ``pytestmark``.
    """
    request.node.add_marker(pytest.mark.regression)
