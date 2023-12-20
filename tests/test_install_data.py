import unittest

import stitches
import stitches.install_pkgdata as sd


class TestInstallRawData(unittest.TestCase):
    """Tests for verifying the installation of raw data."""

    def test_instantiate(self):
        """Test instantiation of InstallPackageData with a fake data directory."""
        zen = sd.InstallPackageData(data_dir="fake")

        # Ensure default version is set
        self.assertEqual(str, type(zen.DEFAULT_VERSION))

        # Ensure URLs are present for current version
        self.assertTrue(stitches.__version__ in zen.DATA_VERSION_URLS)


if __name__ == "__main__":
    unittest.main()
