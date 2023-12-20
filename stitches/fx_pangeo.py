# Define the functions that are useful for working with the pangeo data base
# see https://pangeo.io/index.html for more details.

import fsspec
import intake
import xarray as xr
from tqdm import tqdm


def fetch_pangeo_table():
    """
    Fetch the Pangeo CMIP6 archive table of contents as a pandas DataFrame.

    Retrieve a copy of the Pangeo CMIP6 archive contents, which includes information
    about the available models, sources, experiments, ensembles, and more.

    :return: A pandas DataFrame with details on the datasets available for download from Pangeo.
    """

    # The url path that contains to the pangeo archive table of contents.
    url = "https://storage.googleapis.com/cmip6/pangeo-cmip6.json"
    out = intake.open_esm_datastore(url)

    return out.df


def fetch_nc(zstore: str):
    """
    Extract data for a single file from Pangeo.

    :param zstore: The location of the CMIP6 data file on Pangeo.
    :type zstore: str
    :return: An xarray Dataset containing CMIP6 data downloaded from Pangeo.
    """

    print(f"Fetching: {zstore}")

    # Function to update the progress bar
    def update_progress_bar(mapper, bar):
        keys = list(mapper.keys())

        for key in keys:
            _ = mapper[key]  # Trigger the actual read
            bar.update(1)

    # Create a file system mapper
    mapper = fsspec.get_mapper(zstore)

    # Initialize the progress bar
    with tqdm(
        total=len(mapper.keys()), 
        desc=f"Downloading file components: "
    ) as bar:
        update_progress_bar(mapper, bar)

    # Open the dataset
    ds = xr.open_zarr(mapper)
    ds.sortby("time")

    return ds
