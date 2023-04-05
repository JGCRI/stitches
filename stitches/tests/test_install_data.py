import unittest

import stitches
import stitches.install_pkgdata as sd


class TestInstallRawData(unittest.TestCase):

    def test_instantiate(self):

        zen = sd.InstallPackageData(data_dir="fake")

        # ensure default version is set
        self.assertEqual(str, type(zen.DEFAULT_VERSION))

        # ensure urls present for current version
        self.assertTrue(stitches.__version__ in zen.DATA_VERSION_URLS)


if __name__ == '__main__':
    unittest.main()
