# Define helper functions used through out the package.
import os

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

    out=[]
    for i in a:
        out.append(i[start:stop])
    return out

def join_exclude(d, drop):
    """ Drop some rows from a data frame.

    :param d:   pd data frame containing the data that needs to be dropped.
    :param drop: pd data frame containing the data to drop.

    :return:    pd data frame that is subset.
    """
    # Get the column names that two data frames have
    # in common with one another.
    in_common = list(set(d.columns) & set(drop.columns))    # figure out what columns are in common between the two dfs
    drop["drop"] = 1    # add an indicator column to indicate which rows need to be dropped
    out = d.merge(drop, on=in_common, how="left")   # merge the two df together
    out = out[out["drop"] != 1]     # remove the entries that need to be droped
    out = out[d.columns]    # select the columns

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

