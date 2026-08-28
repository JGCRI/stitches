"""Download and unpack the Zenodo-minted package data for stitches.

The data archive is large (hundreds of megabytes), so the download is streamed to
a temporary file rather than buffered in memory, and progress is reported through
``tqdm``.
"""

import logging
import os
import shutil
import tempfile
import zipfile
from importlib import resources

import requests
from tqdm import tqdm

from ._version import __version__

logger = logging.getLogger(__name__)

#: Seconds to wait for the server to respond before giving up. Applies to the
#: connection and to each read, not to the transfer as a whole, so a slow but
#: progressing download is not killed.
DEFAULT_TIMEOUT = (10, 60)

#: Bytes per streamed chunk.
CHUNK_SIZE = 1024 * 1024


class InstallPackageData:
    """
    Download and unpack example data minted on Zenodo that matches the current installed stitches distribution.

    This class handles the retrieval and organization of example data for the stitches package.
    It ensures that the data version corresponds to the version of the installed stitches package.

    :param data_dir: Optional. Full path to the directory where you wish to store the data.
                     If not specified, the data will be installed in the data directory of the package.
    :type data_dir: str
    :param timeout: Optional. ``(connect, read)`` timeout in seconds passed to
                    ``requests``.
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

    #: Subdirectories that must exist before extraction.
    SUBDIRECTORIES = ("tas-data", "temp-data")

    #: File extensions extracted from the archive.
    KEEP_EXTENSIONS = (".csv", ".nc")

    def __init__(self, data_dir=None, timeout=DEFAULT_TIMEOUT):
        """
        Initialize the InstallPackageData class.

        :param data_dir: The directory where the data will be stored. If None, the data
                         will be installed in the package's data directory.
        :type data_dir: str, optional
        :param timeout: ``(connect, read)`` timeout in seconds.
        """
        self.data_dir = data_dir
        self.timeout = timeout

    def resolve_url(self, version=None):
        """Return the data URL registered for ``version``.

        :param version: Version to look up; defaults to the installed version.
        :return: The download URL.
        :rtype: str
        """
        version = version or __version__

        try:
            return self.DATA_VERSION_URLS[version]
        except KeyError:
            # Previously this silently substituted DEFAULT_VERSION, so a version
            # with no registered dataset would quietly download a possibly
            # mismatched archive. Warn loudly instead; the fallback is retained
            # so that development versions remain usable.
            logger.warning(
                "No data archive is registered for stitches version %s. "
                "Falling back to %s, which may not match this code. "
                "Register the correct URL in InstallPackageData.DATA_VERSION_URLS.",
                version,
                self.DEFAULT_VERSION,
            )
            return self.DEFAULT_VERSION

    def target_directory(self):
        """Return the directory the data will be written into.

        :rtype: str
        """
        if self.data_dir is None:
            return str(resources.files("stitches") / "data")
        return str(self.data_dir)

    def download(self, url, destination):
        """Stream ``url`` to ``destination`` with a progress bar.

        The archive is written to disk incrementally rather than accumulated in
        memory. The previous implementation held the entire response in a
        ``BytesIO``, which needed hundreds of megabytes of RAM and produced no
        progress output despite importing ``tqdm``.

        :param url: URL to fetch.
        :param destination: Local file path to write to.
        :raises requests.HTTPError: If the server returns an error status.
        """
        with requests.get(url, stream=True, timeout=self.timeout) as response:
            # Without this, an HTML error page would be happily written out and
            # then fail later with a confusing "not a zip file" error.
            response.raise_for_status()

            total = int(response.headers.get("content-length", 0))

            with open(destination, "wb") as handle, tqdm(
                total=total or None,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc="Downloading stitches data",
            ) as progress:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    if not chunk:
                        continue
                    handle.write(chunk)
                    progress.update(len(chunk))

    def extract(self, archive_path, data_directory):
        """Extract the wanted members of ``archive_path`` into ``data_directory``.

        Only ``.csv`` and ``.nc`` members are kept, and the archive's directory
        nesting is flattened except that ``tas-data`` and ``temp-data`` members
        are placed in the matching subdirectory.

        :param archive_path: Path to the downloaded zip file.
        :param data_directory: Directory to extract into.
        :return: The list of files written.
        :rtype: list[str]
        """
        written = []

        # Create the destination layout here rather than relying on the caller
        # having done it. `extract` is usable on its own, and a missing
        # subdirectory would otherwise surface as an opaque FileNotFoundError
        # from deep inside shutil.copy.
        os.makedirs(data_directory, exist_ok=True)
        for subdirectory in self.SUBDIRECTORIES:
            os.makedirs(os.path.join(data_directory, subdirectory), exist_ok=True)

        with zipfile.ZipFile(archive_path) as zipped:
            members = [
                name
                for name in zipped.namelist()
                if os.path.splitext(name)[-1] in self.KEEP_EXTENSIONS
            ]

            for name in tqdm(members, desc="Extracting", unit="file"):
                basename = os.path.basename(name)

                # Preserve the subdirectory layout the package expects. The
                # original code handled tas-data only, so temp-data members were
                # flattened into the top level even though the directory was
                # created for them.
                for subdirectory in self.SUBDIRECTORIES:
                    if subdirectory in name:
                        basename = os.path.join(subdirectory, basename)
                        break

                out_file = os.path.join(data_directory, basename)

                with tempfile.TemporaryDirectory() as tdir:
                    zipped.extract(name, tdir)
                    shutil.copy(os.path.join(tdir, name), out_file)

                written.append(out_file)
                logger.debug("Unzipped: %s", out_file)

        return written

    def fetch_zenodo(self):
        """Download and unpack the Zenodo minted data for the current stitches distribution.

        :return: The list of files written.
        :rtype: list[str]
        """
        data_directory = self.target_directory()

        # `os.makedirs(exist_ok=True)` rather than `os.mkdir`, which fails when an
        # intermediate parent is missing and races against concurrent callers.
        for subdirectory in self.SUBDIRECTORIES:
            os.makedirs(os.path.join(data_directory, subdirectory), exist_ok=True)

        data_link = self.resolve_url()

        logger.info(
            "Downloading example data for stitches version %s. This may take a few minutes.",
            __version__,
        )

        # Download to a temporary file so a failed or interrupted transfer cannot
        # leave a truncated archive behind to be mistaken for a good one.
        with tempfile.TemporaryDirectory() as tdir:
            archive_path = os.path.join(tdir, "data.zip")

            self.download(data_link, archive_path)

            try:
                written = self.extract(archive_path, data_directory)
            except zipfile.BadZipFile as exc:
                raise RuntimeError(
                    f"The file downloaded from {data_link} is not a valid zip archive. "
                    "The download may have been truncated, or the URL may no longer "
                    "point at the data archive."
                ) from exc

        logger.info("Wrote %d files to %s", len(written), data_directory)

        return written


def install_package_data(data_dir: str = None):
    """
    Download and unpack Zenodo-minted stitches package data.

    This function matches the current installed stitches distribution and unpacks
    the data into the specified directory or the default data directory of the package.

    :param data_dir: Optional. Full path to the directory to store the data.
                     Default is the data directory of the package.
    :type data_dir: str

    :return: The list of files written.
    :rtype: list[str]
    """
    zen = InstallPackageData(data_dir=data_dir)

    return zen.fetch_zenodo()
