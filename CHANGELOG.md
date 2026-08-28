# Changelog

All notable changes to `stitches` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Output-change policy

`stitches` produces scientific data, so a change in output is a change in
published results. Any modification that alters output must be recorded under a
`### Changed — outputs` heading, must state which defect it corrects, and must be
accompanied by regenerated golden artifacts under `tests/regression/golden/`.
See [`plans/benchmarks-and-regression-testing.md`](plans/benchmarks-and-regression-testing.md).

---

## [Unreleased]

### Fixed

- **`get_chunk_info` crashed on NumPy 2.** The per-chunk rate of change was
  extracted with `float(model.coef_[0])`. Because `LinearRegression` is fitted
  against a column vector, `coef_` has shape `(1, 1)` and `coef_[0]` is a
  one-element array, not a scalar. Converting a one-element array via `float()`
  was deprecated in NumPy 1.25 and raises `TypeError` in NumPy 2, making the
  function — and therefore all archive generation and matching — unusable on
  current NumPy. Now uses `float(model.coef_.ravel()[0])`, verified to produce
  values identical to the legacy behavior across 200 randomized fits.
- **`calculate_rolling_mean` crashed on pandas 3.** The call
  `drop(columns="value", axis=1)` passes both `columns` and `axis`, which pandas
  3.0 rejects with `ValueError: Cannot specify both 'axis' and 'index'/'columns'`.
  The `axis=1` was redundant because `columns=` already selects the column axis;
  it has been removed with no change in behavior.

### Added

- **Golden-output regression suite** (`tests/regression/`) asserting that
  refactors and dependency upgrades do not change scientific output. Artifacts
  are stored as Parquet with per-quantity tolerances (exact for indices, years
  and labels; `1e-12` for match distances; `1e-10` for temperature values).
  Regenerate deliberately with `pytest tests/regression --update-golden`.
  Validated by mutation testing: a one-part-in-10⁹ perturbation of `dist_l2` is
  detected.
- **Explicit `seed` parameters** on `shuffle_function`,
  `permute_stitching_recipes`, and `make_recipe`, so reproducible output no
  longer requires the `testing=True` flag. Fully backward compatible:
  `seed=None` reproduces the previous behavior exactly, and `testing=True`
  remains equivalent to `seed=1`.
- **Pytest capability markers** (`network`, `slow`, `package_data`,
  `regression`, `benchmark`) with `--network`, `--slow`, and `--package-data`
  opt-in flags and matching `STITCHES_TEST_*` environment variables.

### Changed

- **Tests no longer download package data implicitly.** `tests/conftest.py`
  previously installed the full Zenodo archive in a session-scoped `autouse`
  fixture, so every `pytest` invocation downloaded hundreds of megabytes. Package
  data is now an opt-in `package_data` fixture; the offline tier runs in seconds.
- `tests/test_pangeo.py` and `tests/test_stitch.py` converted from
  `unittest.TestCase` to pytest functions, with the monolithic test methods split
  into single-behavior tests.

### Removed

- **The `RUN = "ci"` test gate.** `test_pangeo.py` and `test_stitch.py` guarded
  their real assertions behind a hardcoded class attribute whose false branch ran
  `self.assertEqual(0, 0)`. These tests reported as *passing* in CI while
  exercising no code, leaving the Pangeo and gridded-stitching paths effectively
  untested. They are now marked and skip visibly instead.

### Documentation

- Added `plans/` with a development plan, a git clone-performance analysis, and
  the benchmark/regression design.
