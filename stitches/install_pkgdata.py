from io import BytesIO as BytesIO
import os
import pkg_resources
import shutil
import tempfile
import zipfile

import requests


class InstallPackageData:
    """Download and unpack example data minted on Zenodo that matches the current installed
    stitches distribution.

    :param data_dir:    Optional, Full path oto the directory you wish to store the data in. Default
                        os to install it in the data directory of the package.

    :type data_dir:     str
    """

    # URL for DOI minted example data hosted on Zenodo
    DATA_VERSION_URLS = {'0.9.1':'https://zenodo.org/record/7181977/files/data.zip?download=1'}

    DEFAULT_VERSION = 'https://zenodo.org/record/7181977/files/data.zip?download=1'

    def __init__(self, data_dir=None):

        self.data_dir = data_dir

    def fetch_zenodo(self):
        """Download and unpack the Zenodo minted data for the
        current stitches distribution."""

        # full path to the stitches root directory where the example dir will be stored
        if self.data_dir is None:
            data_directory = pkg_resources.resource_filename('stitches', 'data')
        else:
            data_directory = self.data_dir

        # get the current version of stitches that is installed
        current_version = pkg_resources.get_distribution('stitches').version

        try:
            data_link = InstallPackageData.DATA_VERSION_URLS[current_version]

        except KeyError:
            msg = f"Link to data missing for current version: {current_version}. Using default version: {InstallPackageData.DEFAULT_VERSION}"

            data_link = InstallPackageData.DEFAULT_VERSION

            print(msg)

        # retrieve content from URL
        print("Downloading example data for stitches version {}...".format(current_version))
        r = requests.get(data_link)

        with zipfile.ZipFile(BytesIO(r.content)) as zipped:

            # extract each file in the zipped dir to the project
            for f in zipped.namelist():

                extension = os.path.splitext(f)[-1]

                # Extract only the csv files
                if all([len(extension) > 0, extension == ".csv"]):

                    basename = os.path.basename(f)

                    # Check to see if tas-data is in the file path
                    if "tas-data" in f:
                        basename = "tas-data/" + basename

                    out_file = os.path.join(data_directory, basename)

                    # extract to a temporary directory to be able to only keep the file out of the dir structure
                    with tempfile.TemporaryDirectory() as tdir:

                        # extract file to temporary directory
                        zipped.extract(f, tdir)

                        # construct temporary file full path with name
                        tfile = os.path.join(tdir, f)

                        print(f"Unzipped: {out_file}")
                        # transfer only the file sans the parent directory to the data package
                        shutil.copy(tfile, out_file)


def install_package_data(data_dir=None):
    """Download and unpack Zenodo minted that matches the current installed
    stitches distribution.
    :param data_dir:                    Optional.  Full path to the directory you wish to store the data in.  Default is
                                        to install it in data directory of the package.
    :type data_dir:                     str
    """

    zen = InstallPackageData(data_dir=data_dir)

    zen.fetch_zenodo()
