"""This module contains the InstallPackageData class for downloading and unpacking example data from Zenodo."""

import importlib
import os
import shutil
import tempfile
import zipfile
from importlib import resources
from io import BytesIO as BytesIO

import requests
from tqdm import tqdm

from ._version import __version__


class InstallPackageData:
    """
    Download and unpack example data minted on Zenodo that matches the current installed stitches distribution.

    This class handles the retrieval and organization of example data for the stitches package.
    It ensures that the data version corresponds to the version of the installed stitches package.

    :param data_dir: Optional. Full path to the directory where you wish to store the data.
                     If not specified, the data will be installed in the data directory of the package.
    :type data_dir: str
    """

    # URL for DOI minted example data hosted on Zenodo
    DATA_VERSION_URLS = {
        "0.9.1": "https://zenodo.org/record/7181977/files/data.zip?download=1",
        "0.10.0": "https://zenodo.org/record/7799725/files/data.zip?download=1",
        "0.11.0": "https://zenodo.org/records/8367628/files/data.zip?download=1",
        "0.12.0": "https://zenodo.org/records/8367628/files/data.zip?download=1",
        "0.12.1": "https://zenodo.org/records/8367628/files/data.zip?download=1",
        "0.12.2": "https://zenodo.org/records/8367628/files/data.zip?download=1",
        "0.12.3": "https://zenodo.org/records/8367628/files/data.zip?download=1",
        "0.13": "https://zenodo.org/records/8367628/files/data.zip?download=1",
    }

    DEFAULT_VERSION = "https://zenodo.org/records/8367628/files/data.zip?download=1"

    def __init__(self, data_dir=None):
        """
        Initialize the InstallPackageData class.

        :param data_dir: The directory where the data will be stored. If None, the data
                         will be installed in the package's data directory.
        :type data_dir: str, optional
        """
        self.data_dir = data_dir

    def fetch_zenodo(self):
        """Download and unpack the Zenodo minted data for the current stitches distribution."""
        # full path to the stitches root directory where the example dir will be stored
        if self.data_dir is None:
            data_directory = resources.files("stitches") / "data"
        else:
            data_directory = self.data_dir

        # build needed subdirectories if they do not already exist
        tas_data_path = os.path.join(data_directory, "tas-data")
        temp_data_path = os.path.join(data_directory, "temp-data")
        if not os.path.exists(tas_data_path):
            os.mkdir(tas_data_path)
        if not os.path.exists(temp_data_path):
            os.mkdir(temp_data_path)

        # get the current version of stitches that is installed
        current_version = __version__

        try:
            data_link = InstallPackageData.DATA_VERSION_URLS[current_version]

        except KeyError:
            msg = f"Link to data missing for current version: {current_version}."
            msg += f" Using default version: {InstallPackageData.DEFAULT_VERSION}"

            data_link = InstallPackageData.DEFAULT_VERSION

            print(msg)

        # retrieve content from URL
        print(
            f"Downloading example data for stitches version {current_version}.  This may take a few minutes..."
        )
        response = requests.get(data_link)

        with zipfile.ZipFile(BytesIO(response.content)) as zipped:
            # extract each file in the zipped dir to the project
            for f in zipped.namelist():
                extension = os.path.splitext(f)[-1]

                # Extract only the csv and nc files
                if all([len(extension) > 0, extension in (".csv", ".nc")]):
                    basename = os.path.basename(f)

                    # Check to see if tas-data is in the file path
                    if "tas-data" in f:
                        basename = os.path.join("tas-data", basename)

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


def install_package_data(data_dir: str = None):
    """
    Download and unpack Zenodo-minted stitches package data.

    This function matches the current installed stitches distribution and unpacks
    the data into the specified directory or the default data directory of the package.

    :param data_dir: Optional. Full path to the directory to store the data.
                     Default is the data directory of the package.
    :type data_dir: str

    :return: None
    """
    zen = InstallPackageData(data_dir=data_dir)

    zen.fetch_zenodo()
