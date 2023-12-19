# Define helper functions used through out the package.
import os
from importlib import resources

import numpy as np
import pandas as pd


def combine_df(df1, df2):
    """
    Join two pandas data frames into a single data frame.

    :param df1: First pandas DataFrame.
    :param df2: Second pandas DataFrame.
    :return: A single pandas DataFrame resulting from the joining of df1 and df2.
    """
    incommon = df1.columns.intersection(df2.columns)
    if len(incommon) > 0:
        raise TypeError("a: df1 and df2 must have unique column names.")

    # Combine the two data frames with one another.
    df1["j"] = 1
    df2["j"] = 1
    out = df1.merge(df2)
    out = out.drop(columns="j")

    return out


def list_files(d):
    """
    Return the absolute path for all files in a directory, excluding .DS_Store files.

    :param d: Name of the directory.
    :type d: str
    :returns: List of file paths.
    :rtype: list
    """
    files = os.listdir(d)
    ofiles = []
    for i in range(0, len(files)):
        f = files[i]
        if not (".DS_Store" in f):
            ofiles.append(os.path.join(d, f))
    return ofiles


def selstr(a, start, stop):
    """
    Select elements of a string from start to stop index.

    :param a: Array containing a string.
    :type a: str
    :param start: First character index to select.
    :type start: int
    :param stop: Last character index to select.
    :type stop: int
    :returns: Array of strings.
    :rtype: list
    """
    if type(a) not in [str]:
        raise TypeError("a: must be a single string")

    out = []
    for i in range(start, stop):
        out.append(a[i])
    out = "".join(out)
    return out


def check_columns(data, names):
    """
    Check if a DataFrame contains all required columns.

    :param data: DataFrame to check.
    :type data: pd.DataFrame
    :param names: Set of required column names.
    :type names: set
    :raises TypeError: If `names` is not a set or if required columns are missing.
    """

    col_names = set(data.columns)
    if not (type(names) == set):
        raise TypeError("names must be a set.")

    if not (names.issubset(col_names)):
        raise TypeError(f'Missing columns from "{data}".')


def nrow(df):
    """
    Return the number of rows in the data frame.

    :param df: DataFrame to count rows for.
    :type df: pd.DataFrame
    :return: Number of rows in the data frame.
    :rtype: int
    """

    return df.shape[0]


def remove_obs_from_match(md, rm):
    """
    Return an updated matched data frame to prevent envelope collapse.

    This function is useful for preventing envelope collapse between
    generated and target ensembles by removing observations from the match.

    :param md: Matched data as a pandas DataFrame.
    :param rm: Data to remove as a pandas DataFrame.
    :return: Updated matched data frame as a pandas DataFrame.
    """
    rm = rm[
        [
            "target_year",
            "target_start_yr",
            "target_end_yr",
            "archive_experiment",
            "archive_variable",
            "archive_model",
            "archive_ensemble",
            "archive_start_yr",
            "archive_end_yr",
        ]
    ].copy()
    rm["remove"] = True
    mergedTable = md.merge(rm, how="left")
    key = mergedTable["remove"].isnull()
    out = mergedTable.loc[key][
        [
            "target_variable",
            "target_experiment",
            "target_ensemble",
            "target_model",
            "target_start_yr",
            "target_end_yr",
            "target_year",
            "target_fx",
            "target_dx",
            "archive_experiment",
            "archive_variable",
            "archive_model",
            "archive_ensemble",
            "archive_start_yr",
            "archive_end_yr",
            "archive_year",
            "archive_fx",
            "archive_dx",
            "dist_dx",
            "dist_fx",
            "dist_l2",
        ]
    ]
    return out


def anti_join(x, y, bycols):
    """
    Return a DataFrame of the rows in `x` that do not appear in `y`.

    This function maintains only the columns of `x` with their original names,
    potentially in a different order. It performs an anti-join operation based
    on the specified columns.

    :param x: DataFrame to be filtered.
    :param y: DataFrame to filter against.
    :param bycols: Columns to perform the anti-join on.
    :return: A DataFrame containing the filtered result.
    """
    # Check the inputs
    check_columns(x, set(bycols))
    check_columns(y, set(bycols))

    # select only the entries of x that are not (['_merge'] == 'left_only') in y
    left_joined = x.merge(y, how="left", on=bycols, indicator=True).copy()
    left_only = (
        left_joined.loc[left_joined["_merge"] == "left_only"]
        .drop("_merge", axis=1)
        .copy()
    )

    # left_only has all the columns of x and y, with _x, _y appended to any that
    # had the same names. Want to return left_only with only the columns of x, but
    # which is bycols + anything _x (I think?)
    #
    # first, identify columns that end in _x, subset left_only to just those, and
    # rename the columns to remove the _x:
    _x_ending_cols = [col for col in left_only if col.endswith("_x")]
    left_only_x_ending_cols = left_only[_x_ending_cols].copy()
    new_names = list(
        map(lambda z: z.replace("_x", ""), left_only_x_ending_cols.columns)
    )
    left_only_x_ending_cols.columns = new_names
    #
    # concatenate those with the bycols:
    out = pd.concat([left_only[bycols], left_only_x_ending_cols], axis=1)

    # re-order the columns of out so that they are in the same order
    # as the columns of x
    cols_of_x_in_order = x.columns.copy()
    out = out[cols_of_x_in_order].copy()
    return out


def load_data_files(subdir):
    """
    Read in a list of data frames from a specified subdirectory.

    :param subdir: Subdirectory from which to load data files.
    :type subdir: str
    :return: A single pandas DataFrame object containing concatenated data from all files.
    """
    # Make sure the sub directory exists.
    path = resources.files("stitches") / subdir
    if not os.path.isdir(path):
        raise TypeError("subdir does not exist")

    # Find all of the files.
    files_to_process = list_files(path)
    raw_data = []

    # Read in the data & concatenate into a single data frame.
    for f in files_to_process:

        extension = os.path.splitext(f)[-1].casefold()

        if extension == ".csv":
            d = pd.read_csv(f)
        elif extension == ".pkl":
            d = pd.read_pickle(f)
        else:
            d = None
        raw_data.append(d)
    raw_data = pd.concat(raw_data)

    return raw_data
