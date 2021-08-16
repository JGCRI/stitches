# Define helper functions used through out the package.
import os
import numpy as np
import pandas as pd


def combine_df(df1, df2):
    """ Join the data frames together.

    :param df1:   pandas data frame 1.
    :param df2:   pandas data frame 2.

    :return:    a single pandas data frame.
    """

    # Combine the two data frames with one another.
    df1["j"] = 1
    df2["j"] = 1
    out = df1.merge(df2)
    out = out.drop(columns="j")

    return out


def list_files(d):
    """ Return the absolute path for all of the files in a single directory.

    :param d:   str name of a directory.

    :return:    a list of the files
    """
    files = os.listdir(d)
    ofiles = []
    for i in range(0, len(files)):
        f = files[i]
        ofiles.append(d + '/' + f)

    return ofiles


def selstr(a, start, stop):
    """ Select elements of a string from an array.

    :param a:   array containing a string.
    :param start: int referring to the first character index to select.
    :param stop: int referring to the last character index to select.

    :return:    array of strings
    """

    out = []
    for i in a:
        out.append(i[start:stop])
    return out


def join_exclude(dat, drop):
    """ Drop some rows from a data frame.

    :param dat:   pd data frame containing the data that needs to be dropped.
    :param drop: pd data frame containing the data to drop.

    :return:    pd data frame that is subset.
    """

    # Get the column names that two data frames have
    # in common with one another.
    in_common = list(set(dat.columns) & set(drop.columns))  # figure out what columns are in common between the two dfs
    drop["drop"] = 1  # add an indicator column to indicate which rows need to be dropped
    out = dat.merge(drop, how='inner', on=['experiment', 'model', 'fx', 'dx', 'variable',
                                           'ensemble', 'start_yr', 'end_yr', 'year'])  # merge the two df together

    out = out.loc[out["drop"].isna()]  # remove the entries that need to be dropped

    out = out[d.columns]  # select the columns

    return out


def check_columns(data, names):
    """ Check to see if a data frame has all of the required columns.

    :param data:   pd data
    :param names: set of the required names

    :return:    an error message if there is a colu,n is missing
    """

    col_names = set(data.columns)
    if not (names.issubset(col_names)):
        raise TypeError(f'Missing columns from "{data}".')


def nrow(df):
    """ Return the number of rows

    :param df:   pd data

    :return:    an integer value that corresponds the number of rows in the data frame.
    """

    return df.shape[0]


def anti_join(df1, df2):
    """ Return a data frame that has been created by an anti join.

    :param df1:   pd data
    :param df2:   pd data

    :return:    data frame
    """
    names = list(np.intersect1d(df2.columns, df1.columns))
    df2 = df2[names].copy()
    df2["remove"] = True

    mergedTable = pd.concat([df1, df2], axis=1, join='outer')
    key = mergedTable["remove"].isnull()
    out = mergedTable.loc[key]
    return out[df1.columns]


def remove_obs_from_match(md, rm):
    """ Return an updated matched data frame.

    :param md:   pd data
    :param rm:   pd data

    :return:    data frame
    """
    rm = rm[['target_year', 'target_start_yr', 'target_end_yr', 'archive_experiment',
             'archive_variable', 'archive_model', 'archive_ensemble',
             'archive_start_yr', 'archive_end_yr']].copy()
    rm["remove"] = True
    mergedTable = md.merge(rm, how="left")
    key = mergedTable["remove"].isnull()
    out = mergedTable.loc[key][['target_variable', 'target_experiment', 'target_ensemble',
                                'target_model', 'target_start_yr', 'target_end_yr', 'target_year',
                                'target_fx', 'target_dx', 'archive_experiment', 'archive_variable',
                                'archive_model', 'archive_ensemble', 'archive_start_yr',
                                'archive_end_yr', 'archive_year', 'archive_fx', 'archive_dx', 'dist_dx',
                                'dist_fx', 'dist_l2']]
    return out


def anti_join(x, y):
    """ Return a pd.DataFrame of the rows in x that do not appear in Table y.
    Maintains only the columns of x.
    Adapted from https://stackoverflow.com/questions/38516664/anti-join-pandas

        :param x:   pd.DataFrame object
        :param y:   pd.DataFrame object

        :return:    pd.DataFrame object
        """
    # Identify what values are in TableB and not in TableA
    key_diff = set(x.Key).difference(y.Key)
    where_diff = x.Key.isin(key_diff)

    return(x[where_diff])
