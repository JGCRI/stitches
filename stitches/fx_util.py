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


def anti_join(x, y, bycols):
    """ Return a pd.DataFrame of the rows in x that do not appear in Table y.
    Maintains only the columns of x with their names (but maybe a different
    order?)
    Adapted from https://towardsdatascience.com/masteriadsf-246b4c16daaf#74c6

        :param x:   pd.DataFrame object
        :param y:   pd.DataFrame object
        :param bycols:   list-like; columns to do the anti-join on

        :return:    pd.DataFrame object
        """

    #TODO some test to make sure both x and y have all the columns in bycols



    # select only the entries of x that are not (['_merge'] == 'left_only') in y
    left_joined = x.merge(y, how = 'left', on=bycols, indicator=True).copy()
    left_only = left_joined.loc[left_joined['_merge'] == 'left_only'].drop('_merge', axis=1).copy()

    # left_only has all the columns of x and y, with _x, _y appended to any that
    # had the same names. Want to return left_only with only the columns of x, but
    # which is bycols + anything _x (I think?)
    #
    # first, identify columns that end in _x, subset left_only to just those, and
    # rename the columns to remove the _x:
    _x_ending_cols = [col for col in left_only if col.endswith('_x')]
    left_only_x_ending_cols = left_only[_x_ending_cols].copy()
    new_names = list(map(lambda z: z.replace('_x', ''),
                         left_only_x_ending_cols.columns))
    left_only_x_ending_cols.columns = new_names
    #
    # concatenate those with the bycols:
    out = pd.concat([left_only[bycols], left_only_x_ending_cols],
                    axis=1)

    return out
