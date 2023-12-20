import os
from importlib import resources

import numpy as np
import pandas as pd
import xarray as xr

import stitches.fx_data as data
import stitches.fx_pangeo as pangeo
import stitches.fx_util as util


def find_zfiles(rp):
    """
    Determine which CMIP files must be downloaded from Pangeo.

    :param rp: Data frame of the recipes.
    :return: Numpy ndarray of the gs:// files to pull from Pangeo.
    """

    # Figure out which columns contain the string file
    flat_list = rp.filter(regex="file", axis=1).values.flatten()
    unique_list = np.unique(flat_list)
    return unique_list


def find_var_cols(x):
    """
    Determine the variables to be downloaded.

    :param x: pandas DataFrame of the stitches recipe.
    :return: List of variables to be written to the NetCDF files.
    """

    # Parse out the variable name so that we can use it
    # to label the final output.
    set = x.filter(regex="file").columns.tolist()

    out = []
    for text in set:
        new = text.replace("_file", "")
        out.append(new)
    return out


def get_netcdf_values(i, dl, rp, fl, name):
    """
    Extract archive values from a list of downloaded CMIP data.

    :param i: Index of the row in the recipe data frame.
    :param dl: List of xarray datasets containing CMIP files.
    :param rp: DataFrame of the recipe.
    :param fl: List of CMIP file paths.
    :param name: Name of the variable file to process.
    :return: A slice of xarray data (unsure about the technical term).
    """

    file = rp[name][i]
    start_yr = rp["archive_start_yr"][i]
    end_yr = rp["archive_end_yr"][i]

    # Figure out which index level we are on and then get the
    # xarray from the list.
    index = int(np.where(fl == file)[0])
    extracted = dl[index].sortby("time")
    v = name.replace("_file", "")

    # Have to have special time handler
    times = extracted.indexes["time"]

    if type(times) in [
        xr.coding.cftimeindex.CFTimeIndex,
        pd.core.indexes.datetimes.DatetimeIndex,
    ]:
        yrs = extracted.indexes[
            "time"
        ].year  # pull out the year information from the time index
        flags = list(map(lambda x: x in range(start_yr, end_yr + 1), yrs))
        to_keep = times[flags]
    else:
        raise TypeError("Unsupported time type.")
    dat = extracted.sel(time=to_keep)[v].values.copy()

    if (times.freq == "D") | (times.freq == "day"):
        expected_times = pd.date_range(
            start=str(start_yr) + "-01-01", end=str(end_yr) + "-12-31", freq="D"
        )
        if times.calendar == "noleap":
            expected_len = len(
                expected_times[
                    ~((expected_times.month == 2) & (expected_times.day == 29))
                ]
            )
    else:
        expected_len = len(
            pd.date_range(
                start=str(start_yr) + "-01-01", end=str(end_yr) + "-12-31", freq="M"
            )
        )

    assert len(dat) == expected_len, (
        "Not enough data in " + file + "for period " + str(start_yr) + "-" + str(end_yr)
    )

    return dat


def get_var_info(rp, dl, fl, name):
    """
    Extract the CMIP variable attribute information.

    :param rp: Data frame of the recipes.
    :param dl: List of the data files.
    :param fl: List of the data file names.
    :param name: String of the column containing the variable file name from rp.
    :return: Pandas dataframe of the variable meta data.
    """
    util.check_columns(rp, {name})
    file = rp[name][0]
    index = int(np.where(fl == file)[0])
    extracted = dl[index]

    attrs = data.get_ds_meta(extracted)
    if attrs.frequency.values != "mon":
        attrs["calendar"] = extracted.indexes["time"].calendar

    return attrs


def get_atts(rp, dl, fl, name):
    """
    Extract the CMIP variable attribute information.

    :param rp: Data frame of the recipes.
    :param dl: List of the data files.
    :param fl: List of the data file names.
    :param name: String of the column containing the variable file name from rp.
    :return: Dict object containing the CMIP variable information.
    """
    file = rp[name][0]
    index = int(np.where(fl == file)[0])
    extracted = dl[index]
    v = name.replace("_file", "")

    out = extracted[v].attrs.copy()

    return out


def internal_stitch(rp, dl, fl):
    """
    Stitch a single recipe into netCDF outputs.

    :param dl: List of xarray CMIP files.
    :param rp: DataFrame of the recipe.
    :param fl: List of the CMIP file names.
    :return: List of the data arrays for the stitched products of the different variables.
    """

    rp = rp.sort_values(by=["stitching_id", "target_start_yr"]).copy()
    rp.reset_index(drop=True, inplace=True)
    variables = find_var_cols(rp)
    out = []

    # For each of the of the variables stitch the
    # data together.
    for v in variables:
        # Get the information about the variable that is going to be stitched together.
        col = v + "_file"
        var_info = get_var_info(rp, dl, fl, col)

        # For each of time slices extract the data & concatenate together.
        gridded_data = get_netcdf_values(i=0, dl=dl, rp=rp, fl=fl, name=col)

        # Now add the other time slices.
        for i in range(1, len(rp)):
            new_vals = get_netcdf_values(i=i, dl=dl, rp=rp, fl=fl, name=col)
            gridded_data = np.concatenate((gridded_data, new_vals), axis=0)

        # Note that the pd.date_range call need the date/month defined otherwise it will
        # truncate the year from start of first year to start of end year which is not
        # what we want. We want the full final year to be included in the times series.
        start = str(min(rp["target_start_yr"]))
        end = str(max(rp["target_end_yr"]))

        if var_info["frequency"][0].lower() == "mon":
            freq = "M"
        elif var_info["frequency"][0].lower() == "day":
            freq = "D"
        else:
            raise TypeError("Unsupported frequency.")

        times = pd.date_range(start=start + "-01-01", end=end + "-12-31", freq=freq)

        # Again, some ESMs stop in 2099 instead of 2100 - so we just drop the
        # last year of gridded_data when that is the case.
        # TODO this will need something extra/different for daily data; maybe just
        # a simple len(times)==len(gridded_data)-12 : len(times) == len(gridded_data)-(nDaysInYear)
        # with correct parentheses would do it
        if (max(rp["target_end_yr"]) == 2099) & (
            len(times) == (len(gridded_data) - 12)
        ):
            gridded_data = gridded_data[0 : len(times), 0:, 0:].copy()

        if freq == "D":
            if (var_info["calendar"][0].lower() == "noleap") & (freq == "D"):
                times = times[~((times.month == 2) & (times.day == 29))]

        assert len(gridded_data) == len(times), "Problem with the length of time"

        # Extract the lat and lon information that will be used to structure the
        # empty netcdf file. Make sure to copy all of the information including
        # the attributes!
        lat = dl[0].lat.copy()
        lon = dl[0].lon.copy()

        rslt = xr.Dataset(
            {
                v: xr.DataArray(
                    gridded_data,
                    coords=[times, lat, lon],
                    dims=["time", "lat", "lon"],
                    attrs={
                        "units": var_info["units"][0],
                        "variable": var_info["variable"][0],
                        "experiment": var_info["experiment"][0],
                        "ensemble": var_info["ensemble"][0],
                        "model": var_info["model"][0],
                        "stitching_id": rp["stitching_id"].unique()[0],
                    },
                )
            }
        )

        out.append(rslt)

    out_dict = dict(zip(variables, out))

    return out_dict


def gridded_stitching(out_dir: str, rp):
    """
    Stitch the gridded NetCDFs for variables contained in the recipe file and save them.

    :param out_dir: Directory location where to write the NetCDF files.
    :type out_dir: str
    :param rp: DataFrame of the recipe including variables to stitch.
    :return: List of the NetCDF file paths.
    """

    flag = os.path.isdir(out_dir)
    if not flag:
        raise TypeError("The output directory does not exist.")

    # Check inputs.
    util.check_columns(
        rp,
        {
            "target_start_yr",
            "target_end_yr",
            "archive_experiment",
            "archive_variable",
            "archive_model",
            "archive_ensemble",
            "stitching_id",
            "archive_start_yr",
            "archive_end_yr",
        },
    )

    rp = (
        rp.sort_values(by=["stitching_id", "target_start_yr"])
        .reset_index(drop=True)
        .copy()
    )

    # Determine which variables will be downloaded.
    variables = find_var_cols(rp)
    if not (len(variables) >= 1):
        raise KeyError("No variables were found to be processed.")

    # Determine which files need to be downloaded from pangeo.
    file_list = find_zfiles(rp)

    # Make sure that all of the files are available to download from pangeo.
    # Note that this might be excessively cautious but this is an issue we have run into in
    # the past.
    print("Validating request availability with Pangeo archive contents...")
    avail = pangeo.fetch_pangeo_table()
    flag = all(item in list(avail["zstore"]) for item in list(file_list))
    if not flag:
        raise KeyError("Trying to request a zstore file that does not exist.")

    # Download all of the data from pangeo.
    data_list = list(map(pangeo.fetch_nc, file_list))
    print(f"Compiled downloads into {len(data_list)} datasets.")

    # For each of the stitching recipes go through and stitch a recipe.
    for single_id in rp["stitching_id"].unique():
        # initialize f to be empty just to be safe now that we've added a
        # try...except approach. It's technically possible the first id
        # tried will fail and the function will try to return a non-existent f.
        f = []

        try:
            
            _model = rp.archive_model.unique()
            _var = rp.archive_variable.unique()
            action_msg = f"Stitching gridded netcdf for: {_model} {_var} {single_id} ..."
            print(action_msg)

            # Do the stitching!
            # ** this can be a slow step and prone to errors
            single_rp = rp.loc[rp["stitching_id"] == single_id].copy()

            rslt = internal_stitch(rp=single_rp, dl=data_list, fl=file_list)

            # Print the files out at netcdf files
            f = []
            for i in rslt.keys():
                ds = rslt[i]
                ds = ds.sortby("time").copy()
                recipe_location = (
                    out_dir
                    + "/"
                    + "stitched_"
                    + ds[i].attrs["model"]
                    + "_"
                    + ds[i].attrs["variable"]
                    + "_"
                    + single_id
                    + "_recipe.csv"
                )
                ds[i].attrs["recipe_location"] = recipe_location
                single_rp.to_csv(recipe_location, index=False)
                fname = (
                    out_dir
                    + "/"
                    + "stitched_"
                    + ds[i].attrs["model"]
                    + "_"
                    + ds[i].attrs["variable"]
                    + "_"
                    + single_id
                    + ".nc"
                )
                ds.to_netcdf(fname)
                f.append(fname)
            # end For loop over rslt keys
        # end try

        except:
            print(
                "Stitching gridded netcdf for: "
                + rp.archive_model.unique()
                + " "
                + rp.archive_variable.unique()
                + " "
                + single_id
                + " failed. Skipping. Error thrown within gridded_stitching fxn."
            )
        # end except
    # end for loop over single_id

    return f


def gmat_internal_stitch(row, data):
    """
    Select data from a tas archive based on a single row in a recipe data frame.

    This function is used to iterate over an entire recipe to do the stitching.

    :param row: A row entry of a fully formatted recipe as a pandas Series.
    :param data: A DataFrame containing the tas values to be stitched together.
    :return: A DataFrame of tas values.
    """
    years = list(range(int(row["target_start_yr"]), int(row["target_end_yr"]) + 1))
    select_years = list(
        range(int(row["archive_start_yr"]), int(row["archive_end_yr"]) + 1)
    )

    selected_data = data.loc[
        (data["experiment"] == row["archive_experiment"])
        & (data["year"].isin(select_years))
        & (data["ensemble"] == row["archive_ensemble"])
    ]

    # some models stop at 2099 instead of 2100 - so there is a mismatch
    # between len(years) and selected data but not a fatal one.
    # Write a very specific if statement to catch this & just chop the extra year
    # off the end of selected_data.
    if (len(years) == (util.nrow(selected_data) - 1)) & (max(years) == 2099):
        selected_data = selected_data.iloc[0 : len(years),].copy()

    if len(years) != util.nrow(selected_data):
        raise TypeError("Trouble with selecting the tas data.")

    new_vals = selected_data["value"]
    d = {"year": years, "value": new_vals}
    df = pd.DataFrame(data=d)
    df["variable"] = "tas"

    return df


def gmat_stitching(rp):
    """
    Stitch together a time series of global tas data based on a recipe data frame.

    :param rp: A fully formatted recipe data frame as a pandas DataFrame.
    :return: A pandas DataFrame of stitched together tas data.
    """

    # Check inputs.
    util.check_columns(
        rp,
        {
            "target_start_yr",
            "target_end_yr",
            "archive_experiment",
            "archive_variable",
            "archive_model",
            "archive_ensemble",
            "stitching_id",
            "archive_start_yr",
            "archive_end_yr",
            "tas_file",
        },
    )

    rp = (
        rp.sort_values(by=["stitching_id", "target_start_yr"])
        .reset_index(drop=True)
        .copy()
    )

    # One the assumptions of this function is that it only works with tas, so
    # we can safely add tas as the variable column.
    rp["variable"] = "tas"
    out = []
    for name, match in rp.groupby("stitching_id"):
        # Reset the index in the match data frame so that we can use a for loop
        # to iterate through match data frame an apply the gmat_internal_stitch.
        match = match.reset_index(drop=True)

        # Find the tas data to be stitched together.
        dir_path = resources.files("stitches") / "data" / "tas-data"
        all_files = util.list_files(dir_path)

        # Load the tas data for a particular model.
        model = match["archive_model"].unique()[0]
        csv_to_load = [file for file in all_files if (model in file) and (os.path.basename(file)[0] != ".")][0]

        data = pd.read_csv(csv_to_load)

        # Format the data so that if we have historical years in the future scenarios
        # then that experiment is relabeled as "historical".
        fut_exps = [
            "ssp245",
            "ssp126",
            "ssp585",
            "ssp119",
            "ssp370",
            "ssp434",
            "ssp534-over",
            "ssp460",
        ]
        nonssp_data = data.loc[~data["experiment"].isin(fut_exps)]
        fut_data = data.loc[
            (data["experiment"].isin(fut_exps)) & (data["year"] > 2014)
        ].copy()
        hist_data = data.loc[
            (data["experiment"].isin(fut_exps)) & (data["year"] <= 2014)
        ].copy()
        hist_data["experiment"] = "historical"
        tas_data = (
            pd.concat([nonssp_data, fut_data, hist_data])[
                ["variable", "experiment", "ensemble", "model", "year", "value"]
            ]
            .drop_duplicates()
            .reset_index(drop=True)
        )
        # Stitch the data together based on the matched recipes.
        dat = []
        for i in match.index:
            row = match.iloc[i, :]
            dat.append(gmat_internal_stitch(row, tas_data))
        dat = pd.concat(dat)

        # Add the stitching id column to the data frame.
        dat["stitching_id"] = name

        # Add the data to the out list
        out.append(dat)

    # Format the list of data frames into a single data frame.
    final_output = pd.concat(out)
    final_output = final_output.reset_index(drop=True).copy()
    final_output = final_output.sort_values(["stitching_id", "year"]).copy()
    final_output = final_output.reset_index(drop=True).copy()
    
    return final_output
