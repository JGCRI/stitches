"""This module contains pytest fixtures and configurations for testing the stitches package."""

import pytest

import stitches


@pytest.fixture(scope="session", autouse=True)
def setup_package_data():
    """
    Set up the package data for testing.

    This fixture is automatically used in tests that require the package data.
    It installs the package data and prepares the testing environment.
    """
    stitches.install_package_data()

    return None
