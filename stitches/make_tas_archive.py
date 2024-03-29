"""Module for processing weighted global mean temperature from Pangeo CMIP6 results."""

import os
from importlib import resources

import pandas as pd
from tqdm import tqdm

import stitches.fx_data as data
import stitches.fx_pangeo as pangeo
import stitches.fx_util as util


def join_exclude(dat, drop):
    """Drop some rows from a data frame.

    :param dat:   pd data frame containing the data that needs to be dropped.
    :param drop: pd data frame containing the data to drop.
    :return:    pd data frame that is subset.
    """
    dat = dat.copy()
    drop = drop.copy()

    # Get the column names that two data frames have
    # in common with one another.
    in_common = list(
        set(dat.columns) & set(drop.columns)
    )  # figure out what columns are in common between the two dfs
    drop[
        "drop"
    ] = 1  # add an indicator column to indicate which rows need to be dropped
    out = dat.merge(drop, how="left", on=in_common)

    out = out.loc[out["drop"].isna()]  # remove the entries that need to be dropped
    out = out[dat.columns]  # select the columns

    return out


def rbind(dat1, dat2):
    """Combine two data frames together even if a data frame is empty.

    :param dat1:    data frame of values
    :type dat1:     pandas.core.frame.DataFrame

    :param dat2:    data frame of values
    :type dat2:     pandas.core.frame.DataFrame

    :return: a singular data frame
    """
    if util.nrow(dat1) == 0:
        return dat2

    if util.nrow(dat2) == 0:
        return dat1

    out = pd.concat([dat1, dat2])
    return out


def get_global_tas(path):
    """
    Calculate the weighted annual global mean temperature.

    This function computes the weighted annual global mean surface air temperature
    from CMIP6 files stored on Pangeo.

    :param path: A Zarr store path to the CMIP6 files.
    :type path: str
    :return: Path to the file containing the weighted global mean temperature.
    """
    temp_dir = resources.files("stitches") / "data" / "temp-data"

    if not os.path.isdir(temp_dir):
        os.mkdir(temp_dir)

    tag = path.replace("/", "_")
    file_name = tag.replace("gs:__", "") + "temp.csv"
    ofile = temp_dir + "/" + file_name

    if not os.path.isfile(ofile):
        # Download the CMIP data & calculate the weighted annual global mean .
        d = pangeo.fetch_nc(path)
        global_mean = data.global_mean(d)
        annual_mean = global_mean.coarsen(time=12).mean()

        # Format the CMIP meta data & global means, then combine into a single data frame.
        meta = data.get_ds_meta(d)
        t = annual_mean["time"].dt.strftime("%Y%m%d").values
        year = list(map(lambda x: util.selstr(x, start=0, stop=4), t))

        val = annual_mean["tas"].values
        d = {"year": year, "value": val}
        df = pd.DataFrame(data=d)
        out = util.combine_df(meta, df)

        # Write the output
        out.to_csv(ofile, index=False)
        return ofile
    else:
        return ofile


def calculate_anomaly(data, startYr=1995, endYr=2014):
    """
    Convert CMIP absolute temperature data to anomalies relative to a reference period.

    This function transforms a DataFrame containing CMIP absolute temperature
    data into a DataFrame of temperature anomalies. Anomalies are calculated
    relative to a time-averaged value from a specified reference period.

    :param data: A DataFrame of the CMIP absolute temperature.
    :type data: pandas.core.frame.DataFrame
    :param startYr: The first year of the reference period. Defaults to 1995.
    :type startYr: int
    :param endYr: The final year of the reference period. Defaults to 2014.
    :type endYr: int
    :return: A DataFrame of CMIP temperature anomalies relative to the reference period.
    """
    # Inputs
    util.check_columns(
        data, {"variable", "experiment", "ensemble", "model", "year", "value"}
    )
    to_use = data[["model", "experiment", "ensemble", "year", "value"]].copy()

    # Calculate the average value for the reference period defined
    # by the startYr and ednYr arguments.
    # The default reference period is set from 1995 - 2014.
    #
    # Start by subsetting the data so that it only includes values from the specified reference period.
    to_use["year"] = to_use["year"].astype(int)
    to_use = to_use[to_use["experiment"] == "historical"]
    subset_data = to_use[to_use["year"].between(startYr, endYr)]

    # Calculate the time-averaged reference value for each ensemble
    # realization. This reference value will be used to convert from absolute to
    # relative temperature.
    reference_values = (
        subset_data.groupby(["model", "ensemble"])
        .agg({"value": lambda x: sum(x) / len(x)})
        .reset_index()
        .copy()
    )
    reference_values = reference_values.rename(columns={"value": "ref_values"})

    # Combine the dfs that contain the absolute temperature values with the reference values.
    # Then calculate the relative temperature.
    merged_df = data.merge(reference_values, on=["model", "ensemble"], how="inner")
    merged_df["value"] = merged_df["value"] - merged_df["ref_values"]
    merged_df = merged_df.drop(columns="ref_values")

    return merged_df


def paste_historical_data(input_data):
    """
    Paste historical data into each future scenario.

    This function appends the appropriate historical data to each future scenario,
    ensuring that, for example, SSP585 realization 1 contains data from 1850-2100.

    :param input_data: A DataFrame of the CMIP absolute temperature.
    :type input_data: pandas.core.frame.DataFrame
    :return: A DataFrame of the smoothed time series with a rolling mean applied.
    """
    # Relabel the historical values so that there is a continuous rolling mean between the
    # historical and future values.
    # #######################################################
    # Create a subset of the non historical & future scns
    other_exps = ["1pctCO2", "abrupt-2xCO2", "abrupt-4xCO2"]
    other_data = input_data[input_data["experiment"].isin(other_exps)]

    # Subset the historical data
    historical_data = input_data[input_data["experiment"] == "historical"].copy()

    # Create a subset of the future data
    fut_exps = [
        "ssp126",
        "ssp245",
        "ssp370",
        "ssp585",
        "ssp534-over",
        "ssp119",
        "ssp434",
        "ssp460",
    ]
    future_data = input_data[input_data["experiment"].isin(fut_exps)]
    future_scns = set(future_data["experiment"].unique())

    frames = []
    for scn in future_scns:
        d = historical_data.copy()
        d["experiment"] = scn
        frames.append(d)

    frames.append(future_data)
    frames.append(other_data)
    data = pd.concat(frames)

    d = (
        data.groupby(["variable", "experiment", "ensemble", "model", "year"])["value"]
        .agg("mean")
        .reset_index()
    )

    return d


def make_tas_archive(anomaly_startYr=1995, anomaly_endYr=2014):
    """
    Create the archive from Pangeo-hosted CMIP6 data.

    This function processes CMIP6 data hosted on Pangeo to create an archive of
    temperature anomaly files. It calculates anomalies based on a specified reference
    period.

    :param anomaly_startYr: Start year of the reference period for anomaly calculation.
    :type anomaly_startYr: int
    :param anomaly_endYr: End year of the reference period for anomaly calculation.
    :type anomaly_endYr: int
    :return: List of paths to the created tas files.
    :rtype: list
    """
    # Get the pangeo table of contents.
    df = pangeo.fetch_pangeo_table()

    # Subset the monthly tas data these are the files that we will want to process
    # for the tas archive.
    xps = [
        "historical",
        "1pctCO2",
        "abrupt-4xCO2",
        "abrupt-2xCO2",
        "ssp370",
        "ssp245",
        "ssp119",
        "ssp434",
        "ssp460",
        "ssp126",
        "ssp585",
        "ssp534-over",
    ]
    df = df.loc[
        (df["experiment_id"].isin(xps))
        & (df["table_id"] == "Amon")  # experiments of interest
        & (df["grid_label"] == "gn")  # monthly data
        & (  # we are only interested in the results returned in the native
            df["variable_id"] == "tas"
        )
        & (df["member_id"].str.contains("p1"))  # select temperature data
    ]  # select only the members of the p1 physics group

    # For each of the CMIP6 files to calculate the global mean temperature and write the
    # results to the temporary directory.
    print("Downloading tas data from pangeo...")
    to_download = tqdm(df.zstore.values)
    files = list(map(get_global_tas, to_download))

    print("Processing data this may take a moment.")
    # Clean Up & Quality Control
    #
    # Find all of the files and read in the data, store as a single data frame.
    raw_data = pd.concat(list(map(pd.read_csv, files)))

    # Note that the first three steps only apply to the historical & ssp experiments,
    # the idealized experiments do not need to go through these steps.
    #
    # First round of cleaning check the historical dates.
    # Make sure that the historical run starts some time
    # before 1855 & that that it runs until 2014.
    # Subset the Hector historical
    his_info = (
        raw_data.loc[(raw_data["experiment"] == "historical")]
        .groupby(["model", "experiment", "ensemble"])["year"]
        .agg(["min", "max"])
        .reset_index()
    )
    his_info["min"] = his_info["min"].astype(int)
    his_info["max"] = his_info["max"].astype(int)

    # Make sure the start date is some times before 1855.
    start_yr = his_info[his_info["min"] > 1855].copy()
    to_remove = start_yr[["model", "experiment", "ensemble"]]

    # Make sure that all of historical have data up until 2014
    end_yr = his_info[his_info["max"] < 2014].copy()
    to_remove = rbind(to_remove, end_yr[["model", "experiment", "ensemble"]])
    clean_d1 = join_exclude(raw_data, to_remove)

    # Second round of cleaning check the future dates.
    # Make sure that the future scenarios start at 2015 & run beyond 2100.
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
    fut_info = (
        clean_d1.loc[(clean_d1["experiment"].isin(fut_exps))]
        .groupby(["model", "experiment", "ensemble"])["year"]
        .agg(["min", "max"])
        .reset_index()
    )
    fut_info["min"] = fut_info["min"].astype(int)
    fut_info["max"] = fut_info["max"].astype(int)

    # If the future scenario starts after 2015 drop it.
    start_yr = fut_info[fut_info["min"] > 2015].copy()
    to_remove = start_yr[["model", "experiment", "ensemble"]]

    # Make sure the future scenario runs until 2098 otherwise drop it.
    end_yr = fut_info[fut_info["max"] < 2098].copy()
    to_remove = rbind(to_remove, end_yr[["model", "experiment", "ensemble"]])
    clean_d2 = join_exclude(clean_d1, to_remove)

    # Third round of clean up
    # Make sure that there is data from the historical experiment for each ensemble member with
    # future results.
    exp_en_mod = clean_d2[["experiment", "ensemble", "model"]].drop_duplicates().copy()

    # Separate the data frame of the experiment / ensemble / model information into
    # the historical and non historical experiments.
    hist_ensemble = (
        exp_en_mod.loc[exp_en_mod["experiment"] == "historical"][["model", "ensemble"]]
        .drop_duplicates()
        .copy()
    )
    non_hist_ensemble = (
        exp_en_mod[exp_en_mod["experiment"] != "historical"][["model", "ensemble"]]
        .drop_duplicates()
        .copy()
    )
    # use an inner join to select the historical ensemble that have future results as well as
    # the future results have have historical results.
    to_keep = non_hist_ensemble.merge(
        hist_ensemble, how="inner", on=["ensemble", "model"]
    )

    # Update the raw data table to only include the model / ensembles members that have both a
    # historical and non historical ensemble realization.
    clean_d3 = clean_d2.merge(to_keep, how="inner")

    # Before the fourth round of clean up add back in the idealized experiment results.
    idealized_exps = {"1pctCO2", "abrupt-2xCO2", "abrupt-4xCO2"}
    idealized_dat = raw_data.loc[raw_data["experiment"].isin(idealized_exps)]
    clean_d3 = rbind(clean_d3, idealized_dat)

    # Fourth round of cleaning make sure there are no missing dates.
    yrs = (
        clean_d2.groupby(["model", "experiment", "ensemble"])["year"]
        .agg(["min", "max", "count"])
        .reset_index()
    )
    yrs["min"] = yrs["min"].astype(int)
    yrs["max"] = yrs["max"].astype(int)
    yrs["count"] = yrs["count"].astype(int)
    yrs["diff"] = (yrs["max"] - yrs["min"]) + 1
    yrs["diff"] = yrs["diff"].astype(int)
    to_remove = yrs[yrs["diff"] != yrs["count"]]
    clean_d4 = join_exclude(clean_d3, to_remove).copy()

    # Order the data frame to make sure that all of the years are in order.
    cleaned_data = (
        clean_d4.sort_values(
            by=["variable", "experiment", "ensemble", "model", "year"]
        ).reset_index(drop=True)
    ).copy()

    # Format Data
    #
    # In this section convert from absolute value to an anomaly & concatenate the historical data
    # with the future scenarios.
    data_anomaly = calculate_anomaly(
        cleaned_data, startYr=anomaly_startYr, endYr=anomaly_endYr
    ).copy()
    data = paste_historical_data(data_anomaly)
    data = data.sort_values(by=["variable", "experiment", "ensemble", "model", "year"])
    data = data[
        ["variable", "experiment", "ensemble", "model", "year", "value"]
    ].reset_index(drop=True)

    # Add the z store values to the data frame.
    # Get the pangeo table of contents & assert that the data frame exists and contains information.
    df = pangeo.fetch_pangeo_table()
    df = df.loc[
        (df["table_id"] == "Amon")
        & (df["grid_label"] == "gn")  # monthly data
        & (  # we are only interested in the results returned in the native
            df["variable_id"] == "tas"
        )
        & (df["member_id"].str.contains("p1"))  # select temperature data
    ].copy()  # select only the members of the p1 physics group

    if len(df) <= 0:
        raise Exception("Unable to connect to pangeo, make sure to disconnect from VP")

    # Format the pangeo data frame so that it reflects the contents of data.
    pangeo_df = df[
        ["source_id", "experiment_id", "member_id", "variable_id", "zstore"]
    ].drop_duplicates()
    pangeo_df = pangeo_df.rename(
        columns={
            "source_id": "model",
            "experiment_id": "experiment",
            "member_id": "ensemble",
            "variable_id": "variable",
        }
    )

    # Add the zstore file information to the data frame via a left join.
    data = data.merge(
        pangeo_df, on=["variable", "experiment", "ensemble", "model"], how="inner"
    )

    # Modify the zstore path names to replace the future scn string with historical.
    # TODO replace this for loop it is pretty slow
    new_zstore = []
    for i in data.index:
        # Select the row from the data frame.
        row = data.loc[i]

        # Check to see if the zstore needs to be changed based on if it is a future experiment.
        fut_exps = {
            "ssp119",
            "ssp126",
            "ssp245",
            "ssp370",
            "ssp434",
            "ssp460",
            "ssp534-over",
            "ssp585",
        }
        change = row["experiment"] in fut_exps
        if change:
            new = row["zstore"].replace(row["experiment"], "historical")
        else:
            new = row["zstore"]

        new_zstore.append(new)

    data["zstore"] = new_zstore

    # Save a copy of the tas values, these are the value that will be used to get the
    # tas data chunks. Note that this file has to be compressed so will need to read in
    # using pickle_utils.load()

    files = []
    tas_data_dir = resources.files("stitches") / "data" / "tas-data"
    for name, group in data.groupby(["model"]):
        path = tas_data_dir + "/" + name + "_tas.csv"
        files.append(path)
        group.to_csv(path, index=False)

    print("Global tas data complete")

    return files
