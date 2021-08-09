# Define the collection of helper functions that are used to generate the different
# permutations of the recepies & re-format for stitching.
import pandas as pd

import stitches.fx_util as util
import numpy as np

# Internal
def get_num_perms(matched_data):
    """ A function to give you the number of potential permutations from a
    matched set of data. Ie Taking in the the results of `match_neighborhood(target, archive)`.

        :param matched_data:          data output from match_neighborhood.
        :return:                      A list with two entries. First, the total number of potential permutations of the
        matches that cover 1850-2100 of the  target data in the matched_data dataframe. The second, a data frame with
        the break down of how many matches are in each period of the target data
    """
    # TODO add testing to make sure the matched_data has all
    # the target_ and archive_ columns needed
    util.check_columns(matched_data, {'target_variable', 'target_experiment', 'target_ensemble',
                                      'target_model', 'target_start_yr', 'target_end_yr', 'target_year',
                                      'target_fx', 'target_dx'})

    dat = matched_data.drop_duplicates()
    dat_count = dat.groupby(["target_variable", "target_experiment", "target_ensemble", "target_model",
         "target_start_yr", "target_end_yr", "target_year", "target_fx", "target_dx"]).size().reset_index(name='n_matches')
    dat_count = dat_count.sort_values(["target_year"])

    dat_min = dat_count.groupby(["target_variable", "target_experiment", "target_ensemble", "target_model"])['n_matches'].min().reset_index(name='minNumMatches')
    dat_prod = dat_count.groupby(["target_variable", "target_experiment", "target_ensemble", "target_model"])['n_matches'].prod().reset_index(name='totalNumPerms')
    dat_count_merge = dat_min.merge(dat_prod)

    out = [dat_count_merge, dat_count]
    return out




