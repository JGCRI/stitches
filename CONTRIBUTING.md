# How to contribute

We welcome third-party patches, which are essential for advancing the science and architecture of STITCHES.
But there are a few guidelines that we ask contributors to follow, guidelines that ease the maintainers' organizational and logistical duties, while encouraging development by others. All contributors agree to abide by the code of conduct.

## Development Environment

Because this repository has a large history relative to its working tree, prefer a
blobless clone; it fetches file contents on demand and is substantially faster:

```bash
git clone --filter=blob:none https://github.com/JGCRI/stitches.git
cd stitches

python -m venv .venv && source .venv/bin/activate   # Python >= 3.10
python -m pip install -e ".[dev]"
pre-commit install
```

## Testing

The suite is organized in tiers so the common case is fast and offline.

```bash
pytest                       # offline correctness, a few seconds
pytest tests/regression      # golden-output invariance
pytest --network             # additionally hit Pangeo
pytest --package-data        # additionally download the Zenodo archive
pytest --network --slow --package-data   # everything
```

Capability-gated tests are declared with markers and **skip visibly** when the
capability is not enabled; they never pass vacuously. Each flag has an environment
variable equivalent (`STITCHES_TEST_NETWORK`, `STITCHES_TEST_SLOW`,
`STITCHES_TEST_PACKAGE_DATA`) for CI use.

### Output invariance: the most important rule

`stitches` produces scientific data, so **changing its output changes published
results**. `tests/regression/` compares the current code against golden artifacts
recorded from a known-good baseline. Any refactor, dependency bump, or
optimization must leave these unchanged.

If a regression test fails, first assume you have introduced a bug. Only if the
recorded output is genuinely *wrong* should you regenerate:

```bash
pytest tests/regression --update-golden
```

A pull request that modifies anything under `tests/regression/golden/` **must**:

1. add a `CHANGELOG.md` entry under `### Fixed` or `### Changed — outputs`
   naming the defect the new output corrects, and
2. be reviewed by a domain maintainer, not only a code reviewer.

CI fails the build if golden artifacts change during a test run, so an accidental
`--update-golden` cannot slip through.

When adding a function that produces data, add an invariance test for it.

## Benchmarks

Performance is tracked separately from correctness, and is excluded from the
default `pytest` run:

```bash
pytest benchmarks --benchmark-only --benchmark-save=baseline
pytest benchmarks --benchmark-only --benchmark-compare=baseline
```

Run benchmarks on a single fixed OS and Python version; numbers from different
machines, or from different CI matrix jobs, are not comparable. Optimization work
is only acceptable when the benchmarks improve **and** the invariance suite still
passes. Current baseline measurements and the scaling analysis derived from them
are in [`plans/benchmarks-and-regression-testing.md`](plans/benchmarks-and-regression-testing.md).

## Notebooks

Notebook outputs are stripped automatically on commit by `nbstripout`. This is
deliberate: committed base64 image outputs were the largest single source of
repository growth. Do not re-add them, and do not commit generated data files.
See [`plans/repo-clone-performance.md`](plans/repo-clone-performance.md).

## Getting Started

* Make sure you have a [GitHub account](https://github.com/signup/free).
* **Open an issue** describing your proposed change or work (after making sure one does not already exist).
  * Clearly describe the issue including steps to reproduce when it is a bug.
  * Discuss how your change will affect STITCHES, and thus whether it's MAJOR, MINOR, or a PATCH.
  * Interact with the project maintainers to refine/change/prioritize your issue and identify what branch will be targeted (see below).
* Trivial changes to comments or documentation do not require creating a new issue.

## Making Changes

* **Start your work on the correct branch**.
  * The active development branch will be titled dev
  * If your change is a PATCH, it will typically be based on the current dev branch; if MINOR, the next minor release branch; if MAJOR, the next major release branch. For example, as of this writing there are branches `dev`, `rc1.2` and `rc2.0`, corresponding to the PATCH-MINOR-MAJOR start points respectively.
  * We will never accept pull requests to the `master` branch.
* Check for unnecessary whitespace with `git diff --check` before committing.
* Make sure your commit messages are descriptive but succinct, describing what was changed and why, and **reference the relevant issue number**. Make commits of logical units.
* Make sure you have added the necessary tests for your changes. Tests belong in the root `tests` directory and run under `pytest`, which is installed with the development extra. See the [`pytest` documentation](https://docs.pytest.org/).
* If your change touches code that produces data, add or update an invariance test in `tests/regression/` (see **Output invariance** above).
* Run _all_ the tests to assure nothing else was accidentally broken.
* Record notable changes in `CHANGELOG.md`. Anything that alters output **must** be recorded there.

## Submitting Changes

* Submit a pull request.
* **Your pull request should include one of the following two statements**:
   * You own the copyright on the code being contributed, and you hereby grant PNNL unlimited license to use this code in this version or any future version of STITCHES. You reserve all other rights to the code.
   * Somebody else owns the copyright on the code being contributed (e.g., your employer because you did it as part of your work for them); you are authorized by that owner to grant PNNL an unlimited license to use this code in this version or any future version of STICHES, and you hereby do so. All other rights to the code are reserved by the copyright owner.
* The core team looks at Pull Requests on a regular basis, and will respond as soon as possible.


# Additional Resources

* [General GitHub documentation](http://help.github.com/)
* [GitHub pull request documentation](http://help.github.com/send-pull-requests/)
