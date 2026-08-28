"""Pytest fixtures and configuration for the stitches test suite.

Design goals:

* **No implicit network access.** The previous version of this module installed
  the full Zenodo-minted package data in a session-scoped ``autouse`` fixture,
  which meant every ``pytest`` invocation -- including a run of a single pure
  unit test -- downloaded hundreds of megabytes. Package data is now an
  explicit, opt-in fixture (``package_data``) that individual tests request.
* **No silent skips.** Tests that need the network or the full data archive are
  marked (``network``, ``package_data``, ``slow``) and are deselected with a
  visible skip reason rather than passing vacuously.

Opt-in mechanisms, in precedence order:

===================  =========================  ================================
Capability           CLI flag                   Environment variable
===================  =========================  ================================
Network access       ``--network``              ``STITCHES_TEST_NETWORK=1``
Long-running tests   ``--slow``                 ``STITCHES_TEST_SLOW=1``
Full package data    ``--package-data``         ``STITCHES_TEST_PACKAGE_DATA=1``
===================  =========================  ================================

``--package-data`` implies ``--network`` on first use, since the archive must be
downloaded before it can be used.
"""

import os
from importlib import resources
from pathlib import Path

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Command line options
# ---------------------------------------------------------------------------

#: Maps a marker name to its ``(cli flag, environment variable)`` pair.
_OPT_IN_MARKERS = {
    "network": ("--network", "STITCHES_TEST_NETWORK"),
    "slow": ("--slow", "STITCHES_TEST_SLOW"),
    "package_data": ("--package-data", "STITCHES_TEST_PACKAGE_DATA"),
}


def pytest_addoption(parser):
    """Register the opt-in flags for capability-gated tests."""
    group = parser.getgroup("stitches")
    group.addoption(
        "--network",
        action="store_true",
        default=False,
        help="Run tests marked 'network' that require internet access.",
    )
    group.addoption(
        "--slow",
        action="store_true",
        default=False,
        help="Run tests marked 'slow' (>30s wall clock).",
    )
    group.addoption(
        "--package-data",
        action="store_true",
        default=False,
        help=(
            "Run tests marked 'package_data', downloading the Zenodo-minted "
            "archive if it is not already present. Implies --network."
        ),
    )
    group.addoption(
        "--update-golden",
        action="store_true",
        default=False,
        help=(
            "Regenerate golden regression artifacts from the current code "
            "instead of asserting against them. Use only when an output change "
            "is intentional, and record the justification in CHANGELOG.md."
        ),
    )


def _enabled(config, marker):
    """Return True when the capability behind ``marker`` has been opted into."""
    flag, env_var = _OPT_IN_MARKERS[marker]
    if config.getoption(flag.lstrip("-").replace("-", "_")):
        return True
    if os.environ.get(env_var, "").strip().lower() in {"1", "true", "yes"}:
        return True
    # Downloading package data necessarily requires the network, so enabling
    # package data also enables the network capability.
    if marker == "network" and _enabled(config, "package_data"):
        return True
    return False


def pytest_collection_modifyitems(config, items):
    """Skip capability-gated tests unless the capability was opted into."""
    for marker in _OPT_IN_MARKERS:
        if _enabled(config, marker):
            continue
        flag, env_var = _OPT_IN_MARKERS[marker]
        skip = pytest.mark.skip(
            reason=f"needs {marker!r}: pass {flag} or set {env_var}=1 to run"
        )
        for item in items:
            if marker in item.keywords:
                item.add_marker(skip)


# ---------------------------------------------------------------------------
# Data fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def example_data_dir():
    """Return the directory holding the small committed example CSV fixtures.

    These files ship inside the package and require no download, so they back
    the offline tier of the test suite.
    """
    return Path(str(resources.files("stitches") / "data" / "example"))


@pytest.fixture(scope="session")
def read_example(example_data_dir):
    """Return a loader for a committed example CSV by stem name.

    Usage::

        def test_something(read_example):
            target = read_example("test-target_dat")
    """

    def _read(name):
        stem = name[:-4] if name.endswith(".csv") else name
        path = example_data_dir / f"{stem}.csv"
        if not path.is_file():
            raise FileNotFoundError(f"No example fixture named {stem!r} at {path}")
        return pd.read_csv(path)

    return _read


@pytest.fixture(scope="session")
def package_data():
    """Ensure the full Zenodo-minted package data is installed.

    Tests requesting this fixture must also carry the ``package_data`` marker so
    that they are deselected when the capability is not opted into. The download
    is performed at most once per session and is skipped when the data already
    appears to be present.
    """
    import stitches

    data_dir = Path(str(resources.files("stitches") / "data"))
    sentinel = data_dir / "matching_archive.csv"

    if not sentinel.is_file():
        stitches.install_package_data()

    if not sentinel.is_file():
        pytest.fail(
            f"package data installation did not produce {sentinel}; "
            "cannot run package_data-marked tests"
        )

    return data_dir
