# Helper functions designed to help manipulate the netcdf files downloaded from the
# pangeo.

import numpy as np

def get_var_names(set):
    """ Get the variable name from the file name inormation .

        :param set:            a set of the strings describing the file names
        :return:               a set off strings containing the cmip variable name
    """
    out = []
    for text in set:
        new = text.replace("_file", "")
        out.append(new)
    return out

def get_attr_info(rp, dl, fl, name):
    """Extract the cmip variable attribute information.

           :param rp:             data frame of the recepies
           :param dl:             list of the data files
           :param fl:             list of the data file names
           :param name:           string of the column containing the variable files to process
           :return:               dict object containing the cmip variable information

           TODO add a check to make sure that there is only one stitching id being passed into
           the function.
       """
    file = rp[name][0]
    index = int(np.where(fl == file)[0])
    extracted = dl[index]
    v=name.replace("_file", "")

    out=extracted[v].attrs.copy()

    return out