# Define the collection of helper functions that are used to generate the different
# permutations of the recipes & re-format for stitching.
import pandas as pd
import stitches.fx_util as util
import stitches.fx_match as match
# import pickle_utils as pickle


# Internal
def get_num_perms(matched_data):
    """ A function to give you the number of potential permutations from a
    matched set of data. Ie Taking in the the results of `match_neighborhood(target, archive)`.

        :param matched_data:          data output from match_neighborhood.
        :return:                      A list with two entries. First, the total number of potential permutations of the
        matches that cover 1850-2100 of the  target data in the matched_data dataframe. The second, a data frame with
        the break down of how many matches are in each period of the target data
    """
    # Check inputs
    util.check_columns(matched_data, {'target_variable', 'target_experiment', 'target_ensemble',
                                      'target_model', 'target_start_yr', 'target_end_yr', 'target_year',
                                      'target_fx', 'target_dx'})

    dat = matched_data.drop_duplicates()
    dat_count = dat.groupby(["target_variable", "target_experiment", "target_ensemble", "target_model",
                             "target_start_yr", "target_end_yr", "target_year", "target_fx",
                             "target_dx"]).size().reset_index(name='n_matches')
    dat_count = dat_count.sort_values(["target_year"])

    dat_min = dat_count.groupby(["target_variable", "target_experiment", "target_ensemble", "target_model"])[
        'n_matches'].min().reset_index(name='minNumMatches')
    dat_prod = dat_count.groupby(["target_variable", "target_experiment", "target_ensemble", "target_model"])[
        'n_matches'].prod().reset_index(name='totalNumPerms')
    dat_count_merge = dat_min.merge(dat_prod)

    out = [dat_count_merge, dat_count]
    return out


# TODO ACS please review carefuly
def remove_duplicates(md, archive, drop_hist_duplicates=False):
    """ A function that makes sure that within a single given matched recipe that
        there each archive point used is unique. When two target tgav windows in
        the trajectory match to the same archive window, the target window with
        smaller Euclidean distance keeps the match, and the other target window
        gets re-matched with its nearest-neighbor match from a new archive, the
        previous one with all matched points removed.
        
        :param md:          A data frame with results of matching for a single
                            tgav recipe. Either because match_neighborhood was
                            used specifically to return NN or because the multiple
                            matches have been permuted into new recipes and then
                            split with this function being applied to each recipe.
        :param archive:     data frame object consisting of the tas archive to use
                            for re-matching duplicate points.
        :param drop_hist_duplicates:    boolean True/False to indicate if the drop_hist_duplicates function should be triggered.
        :return:                       data frame with same structure as raw matched, with duplicate matches replaced.
    """
    if len(md["target_year"].unique()) < util.nrow(md):
        raise TypeError(f"You have multiple matches to a single target year, this function can only accept a matched "
                        f"data frame of singular matches between target & archive data.")

    # Check to see if in the matched data frame if there are any repeated values.
    md_archive = md[['archive_experiment', 'archive_variable', 'archive_model',
                     'archive_ensemble', 'archive_start_yr', 'archive_end_yr',
                     'archive_year', 'archive_fx', 'archive_dx']]
    duplicates = md.merge(md_archive[md_archive.duplicated()], how="inner")

    if util.nrow(duplicates) == 0:
        matched_data = md.copy()

    # If none of the archive points are being used more than once in the
    # matched ts we can continue on. However we find that that a single archive
    # point is being used more than once within a ts extra steps will have to be taken.
    # TODO ACS please take a look, didn't have the data to test this out...
    while util.nrow(duplicates) > 0:

        # within each iteration of checking duplicates,
        # pull out the one with smallest dist_l2 -
        # this is the one that gets to keep the match, and we use
        # as an index to work on the complement of (in case the same
        # archive point gets matched for more than 2 target years)
        grouped = duplicates.groupby(['archive_experiment', 'archive_variable',
                                      'archive_model', 'archive_ensemble',
                                      'archive_start_yr', 'archive_end_yr', 'archive_year',
                                      'archive_fx', 'archive_dx'])
        # Pick which of the target points will continue to be matched with the archive
        # pair.
        dat = []
        for name, group in grouped:
            min_value = min(group['dist_l2'])
            dat.append(group.loc[group['dist_l2'] == min_value])
        duplicates_min = pd.concat(dat)

        # target points of duplicates-duplicates_min need to be
        # refit on the archive, the  ones that need a new pairing.
        filter_col = [col for col in duplicates if col.startswith('target_')]
        points_to_rematch = duplicates[filter_col].loc[(~duplicates['target_year'].isin(duplicates_min['target_year']))]
        new_names = list(map(lambda x: x.replace('target_', ''), points_to_rematch.columns))
        points_to_rematch.columns = new_names

        # Because we know that none of the archive values can be reused in the match discard them
        # from the updated archive that will be used in the rematching.
        cols = [col for col in md if col.startswith('archive_')]
        rm_from_archive = md[cols]
        new_names = list(map(lambda x: x.replace('archive_', ''), rm_from_archive.columns))
        rm_from_archive.columns = new_names

        # TODO something might be up with this approach to approximate an anti join,
        # My python take on an anti join, combine two data frames together into a single data
        # frame that has duplicates of the rows we would like to remove. Then use the drop
        # duplicates function to discard those rows so now that data frame only contains unique
        # entries.
        new_archive = pd.concat([archive[['model', 'experiment', 'variable', 'ensemble',
                                          'start_yr', 'end_yr', 'year', 'fx', 'dx']],
                                 rm_from_archive[['model', 'experiment', 'variable', 'ensemble',
                                                  'start_yr', 'end_yr', 'fx', 'dx']]]).drop_duplicates(
            subset=['model', 'experiment',
                    'variable', 'ensemble',
                    'start_yr', 'end_yr'], keep=False)

        # Find new matches for the data the target data that is missing the archive pair. Because we
        # are only interested in completing our singular recipe the tol must be 0.
        rematched = match.match_neighborhood(target_data=points_to_rematch, archive_data=new_archive,
                                             tol=0, drop_hist_duplicates=drop_hist_duplicates)

        # Add back in the rematched data.
        matched_data = pd.concat([md.loc[~md['target_year'].isin(rematched['target_year'])],
                                  rematched])[['target_variable', 'target_experiment', 'target_ensemble',
                                               'target_model', 'target_start_yr', 'target_end_yr', 'target_year',
                                               'target_fx', 'target_dx', 'archive_experiment', 'archive_variable',
                                               'archive_model', 'archive_ensemble', 'archive_start_yr',
                                               'archive_end_yr', 'archive_year', 'archive_fx', 'archive_dx', 'dist_dx',
                                               'dist_fx', 'dist_l2']].sort_values('target_year').reset_index()

        md_archive = matched_data[['archive_experiment', 'archive_variable', 'archive_model',
                                   'archive_ensemble', 'archive_start_yr', 'archive_end_yr',
                                   'archive_year', 'archive_fx', 'archive_dx']]
        duplicates = matched_data.merge(md_archive[md_archive.duplicated()], how="inner")

        # Clean up for the next while loop iteratio
        # del(duplicates_min, points_to_rematch, rm_from_archive, rematched)

    return matched_data


# TODO this function has some ISSUES!
def permute_stitching_recipes(N_matches, matched_data, archive, optional=None):
    """  A function to sample from input `matched_data` (the the results
    of `match_neighborhood(target, archive)` to produce permutations
    of possible stitching recipes that will match the target data.

        :param N_matches:       int the number of desired stitching recipes.
        :param matched_data:    data output from match_neighborhood.
        :param archive:         the archive data to use for re-matching duplicate points
        :param optional:        a previous output of this function that contains a list of already created recipes
                                to avoid re-making (this is not implemented).

        :return:                data frame with same structure as raw matched, with duplicate matches replaced.
    """

    # Check inputs
    util.check_columns(matched_data, {'target_variable', 'target_experiment', 'target_ensemble',
                                      'target_model', 'target_start_yr', 'target_end_yr', 'target_year',
                                      'target_fx', 'target_dx', 'archive_experiment', 'archive_variable',
                                      'archive_model', 'archive_ensemble', 'archive_start_yr',
                                      'archive_end_yr', 'archive_year', 'archive_fx', 'archive_dx', 'dist_dx',
                                      'dist_fx', 'dist_l2'})
    matched_data = matched_data.drop_duplicates().copy()

    num_target_windows = util.nrow(matched_data["target_year"].unique())
    num_perms = get_num_perms(matched_data)

    # TODO ACS is the num perms the same for the two different targets? cause
    # they are the same experiment but different ensemble realizations?
    perm_guide = num_perms[1]

    # how many target trajectories are we matching to,
    # how many collapse-free ensemble members can each
    # target support, and order them according to that
    # for construction.
    targets = num_perms[0].sort_values(["minNumMatches"]).reset_index()
    # Add a column of a target  id name, differentiate between the different input
    # streams we are emulating.
    targets['target_ordered_id'] = [chr(ord('a') + x).upper() for x in targets.index]

    if util.nrow(num_perms[0]["target_experiment"].unique()) > 1:
        raise TypeError(
            f"function permute_stitching_recipes should be applied to separate data frames for each target experiment of interest (multiple target ensemble members for a single target experiment is fine)")

    # max number of permutations per target without repeating across generated
    # ensemble members.
    N_data_max = min(num_perms[0]['minNumMatches'])

    if N_matches > N_data_max:
        # TODO this should be written up as a proper python message statement.
        print("You have requested more recipes than possible for at least one target trajectories, returning what can")

    # Initialize the number of matches to either 0 or the input read from optional:
    if type(optional) is str:
        print('initialize to the read-in: has not been translated')
    else:
        recipe_collection = pd.DataFrame()

    for target_id in targets['target_ordered_id'].unique():
        # subset the target info, the target df contains meta information about the run we
        # and the number of permutations and such.
        target = targets.loc[targets["target_ordered_id"] == target_id].copy()

        # initialize a recipes for each target
        recipes_col_by_target = pd.DataFrame()
        var = target['target_variable'].unique()[0]
        exp = target['target_experiment'].unique()[0]
        mod = target['target_model'].unique()[0]
        ens = target['target_ensemble'].unique()[0]

        # While the following conditions are met continue to generate new recipes.
        # 1. While we have fewer matches than requested for the target ensemble_member,
        #    keep going.
        # 2. Filter the perm_guide to just the target ensemble member in this loop and
        #    make sure there are at least num_target_windows of time windows: basically
        #    make sure there is at least one remaining archive match to draw from for
        #    each target window in this target ensemble. Note this means the perm_guide
        #    must be updated at the end of every while loop iteration.
        if util.nrow(recipes_col_by_target) == 0:
            condition1 = True
        elif util.nrow(recipes_col_by_target['stitching_id'].unique()) < N_matches:
            condition1 = True
        else:
            condition1 = False

        perm_rows = util.nrow(
            perm_guide.loc[(perm_guide['target_variable'] == var) & (perm_guide['target_experiment'] == exp) &
                           (perm_guide['target_model'] == mod) & (perm_guide['target_ensemble'] == ens)]
                .copy()
                .drop_duplicates())

        if perm_rows == num_target_windows:
            condition2 = True
        else:
            condition2 = False

        # The index is used to indicate which number of match is being generated
        index = 1

        # Run the while loop!
        while all([condition1, condition2]):

            # Group matched data for a single target by the chunks of the target information.
            # Right now a single target chunk may have multiple matches with archive points. The
            # next several steps of the while loop will create a one to one paring between the
            # target and archive data, then check to make sure that the pairing meets the requirements
            # for what we call a recipe.
            grouped_targets = matched_data.loc[(matched_data['target_variable'] == var) &
                                               (matched_data['target_experiment'] == exp) &
                                               (matched_data['target_model'] == mod) &
                                               (matched_data['target_ensemble'] == ens)].copy().groupby(
                ['target_variable', 'target_experiment', 'target_ensemble', 'target_model',
                 'target_start_yr', 'target_end_yr'])

            # Randomly select which of the archive matches to use in the c, aka make this
            # a one-to-one match.
            one_one_match = []
            for name, group in grouped_targets:
                one_one_match.append(group.sample(1, replace=False))
            one_one_match = pd.concat(one_one_match)
            # TODO add a check to make sure that the correct number of rows are being returned.

            # Before we have a potential c make sure it meets our first condition,
            # that each archive data point in the recipe must be unique.
            new_recipe = remove_duplicates(one_one_match, archive)
            stitching_id = exp + '~' + ens + '~' + target_id + str(index)
            new_recipe["stitching_id"] = stitching_id

            # Make sure the sampled_match (the new recipe isn't missing any years)
            if ~new_recipe.shape[0] == num_target_windows:
                raise TypeError(f"problem the new single recipe is missing years of data!")
            #  Make sure that no changes were made to the target years.
            if sum(~new_recipe['target_start_yr'].isin(set(matched_data['target_start_yr']))) > 0:
                raise TypeError(f"problem the new single recipe target years!")

            # Now check to make sure that that our second condition is met, that across
            # the ensemble of recipes (the collection of recipes), each recipe is unique.
            if util.nrow(recipes_col_by_target) == 0:
                # If this is the first entry in teh recipe collection add it.
                recipes_col_by_target = pd.concat([recipes_col_by_target, new_recipe])
            else:
                # Compare the new recipe with the existing collection. We only care that about the
                # recipes_col_by_target data so can exclude fx and dx in our comparison.
                cols_to_use = ['target_variable', 'target_experiment', 'target_ensemble',
                               'target_model', 'target_start_yr', 'target_end_yr', 'archive_experiment',
                               'archive_variable', 'archive_model', 'archive_ensemble', 'archive_start_yr',
                               'archive_end_yr']
                grouped_collection = recipes_col_by_target.groupby(['stitching_id'])
                comparison = []
                for name, group in grouped_collection:
                    df1 = group[cols_to_use].copy().reset_index(drop=True).sort_index(axis=1)
                    df2 = new_recipe[cols_to_use].copy().reset_index(drop=True).sort_index(axis=1)
                    comparison.append(all(df1 == df2))

                if any(comparison):
                    # If the new_recipe is not unique then do nothing
                    print('rejected: ' + stitching_id)
                    # okay so i think python may be unhappy with an empty if else
                    # statement, might want to rethink this into jsut a simple if statement.
                else:
                    # Our sampled_match didn't match any previous recipes,
                    # so we are safe to add it to the collection.
                    print('good recipe')
                    recipes_col_by_target = pd.concat([recipes_col_by_target, new_recipe])

                    # And we remove it from the matched_points so the archive
                    # values used in this sampled_match
                    # can't be used to construct subsequent realizations.
                    # This updated matched_data is used in each iteration
                    # of the while loop. Since we are removing the constructed
                    # sampled_match from the matched_data at the end of each
                    # iteration of the while loop, the sample points can't be
                    # randomly drawn again for the next generated trajectory
                    # of the current target ensemble member for loop iteration.

            # Now each (target_window, archive_window) combination must
            # be removed from matched data for all target_ids
            to_remove = new_recipe[["target_year", "target_start_yr", "target_end_yr",
                                    "archive_experiment", "archive_variable", "archive_model",
                                    "archive_ensemble", "archive_start_yr", "archive_end_yr",
                                    "archive_year"]].copy()
            # Use an anti-join
            matched_data = util.remove_obs_from_match(matched_data, to_remove).copy()

            # update permutation count info with the
            # revised matched data so the while loop behaves - this makes sure
            # that every target window in the perm_guide actually has at least
            # one matched archive point available for draws (the while loop condition).
            # That way, we don't try to construct a trajectory with fewer years
            # than the targets.
            num_perms = get_num_perms(matched_data)
            perm_guide = num_perms[1]

            # Check the while loop conditions
            if util.nrow(recipes_col_by_target) == 0:
                condition1 = True
            elif util.nrow(recipes_col_by_target['stitching_id'].unique()) < N_matches:
                condition1 = True
            else:
                condition1 = False

            perm_rows = util.nrow(
                perm_guide.loc[(perm_guide['target_variable'] == var) & (perm_guide['target_experiment'] == exp) &
                               (perm_guide['target_model'] == mod) & (perm_guide['target_ensemble'] == ens)]
                    .copy()
                    .drop_duplicates())

            if perm_rows == num_target_windows:
                condition2 = True
            else:
                condition2 = False

            # Add to the index, this is used to indicate which ensemble of the recpie.
            index += 1

        # Add the collection of the recipes for each of the targets into single df.
        recipe_collection = recipe_collection.append(recipes_col_by_target)

    # End of for loop over that runs over the target for loops
    out = recipe_collection.reset_index(drop=True).copy()
    return out[['target_variable', 'target_experiment', 'target_ensemble',
                'target_model', 'target_start_yr', 'target_end_yr', 'target_year',
                'target_fx', 'target_dx', 'archive_experiment', 'archive_variable',
                'archive_model', 'archive_ensemble', 'archive_start_yr',
                'archive_end_yr', 'archive_year', 'archive_fx', 'archive_dx', 'dist_dx',
                'dist_fx', 'dist_l2', 'stitching_id']]


def handle_transition_periods(rp):
    """ Go through the recipe and when there is a transition period, aka the archive years span both the
    historical and future scenarios go through and insert in an extra period so that they don't do
    this over lap any more.

        :param rp:       a data frame of the recipe.

        :return:         a data frame of of the recipe with no over lapping historical/future experiments, this is now ready to join with pangeo information.
    """
    util.check_columns(rp, {'target_variable', 'target_experiment', 'target_ensemble',
                            'target_model', 'target_start_yr', 'target_end_yr', 'target_year',
                            'target_fx', 'target_dx', 'archive_experiment', 'archive_variable',
                            'archive_model', 'archive_ensemble', 'archive_start_yr',
                            'archive_end_yr', 'archive_year', 'archive_fx', 'archive_dx', 'dist_dx',
                            'dist_fx', 'dist_l2', 'stitching_id'})

    def internal_func(x):
        # First check to see the archive period spans the historical to future scenariox[]
        transition_period = 2014 in range(x["archive_start_yr"], x["archive_end_yr"])

        if transition_period:
            target_yrs = list(range(x['target_start_yr'], x['target_end_yr']))
            archive_yrs = list(range(x['archive_start_yr'], x['archive_end_yr']))
            hist_cut_off = 2014  # the final complete year of the historical experiment

            historical_yrs = list(filter(lambda x: x <= hist_cut_off, archive_yrs))
            future_yrs = list(set(archive_yrs).difference(set(historical_yrs)))

            # This is the information that is constant between the historical and future periods.
            constant_info = x.loc[x.index.isin({'archive_variable', 'archive_model',
                                                'archive_ensemble', 'stitching_id'})]

            # Construct the historical period information
            d = {'target_start_yr': min(target_yrs),
                 'target_end_yr': target_yrs[len(historical_yrs)],
                 'archive_experiment': 'historical',
                 'archive_start_yr': min(historical_yrs),
                 'archive_end_yr': max(historical_yrs)}
            ser = pd.Series(data=d, index=['target_start_yr', 'target_end_yr', 'archive_experiment',
                                           'archive_start_yr', 'archive_end_yr'])
            historical_period = ser.append(constant_info).to_frame().transpose()

            # Now construct the future period information
            d = {'target_start_yr': target_yrs[len(historical_yrs)],
                 'target_end_yr': target_yrs[len(target_yrs) - 1],
                 'archive_experiment': 'historical',
                 'archive_start_yr': min(future_yrs),
                 'archive_end_yr': max(future_yrs)}
            ser = pd.Series(data=d, index=['target_start_yr', 'target_end_yr', 'archive_experiment',
                                           'archive_start_yr', 'archive_end_yr'])
            future_period = ser.append(constant_info).to_frame().transpose()

            # Combine the period information
            out = pd.concat([historical_period, future_period]).reset_index(drop=True)
        else:
            out = x.to_frame().transpose().reset_index(drop=True)
            out = out[['target_start_yr', 'target_end_yr', 'archive_experiment', 'archive_variable',
                       'archive_model', 'archive_ensemble', 'stitching_id', 'archive_start_yr',
                       'archive_end_yr']]

        return out

    # Note that data frame returned might not be identical in shape to the
    # recipe read in because any periods that cover the historical period
    # will be split into two rows.
    ser = rp.apply(internal_func, axis=1)
    out = pd.concat(ser.values.tolist()).reset_index(drop=True)
    return out


def handle_final_period(rp):
    """ Go through a recipe and ensure that all of the periods have the same archive 
    and target period length, if not update to reflect the target period length. 
    Otherwise you'll end up with extra years in the stitched data. This is really 
    only an issue for the final period of target data because sometimes that period is somewhat short. 
    OR if the normal sized target window gets matched to the final period of data from one
    of the archive matches. Since the final period is typically pnly one year shorter than the 
    full window target period in this case, we simply repeat the final archive year to get 
    enough matches.

        :param rp:       a data frame of the recipe.

        :return:         a recipe data frame that has target and archive periods of the same length.
    """
    # Define an internal function that checks row by row if we are working
    # with the final period & if that is a problem, if so handle it.
    def internal_func(x):
        len_target = x['target_end_yr'] - x['target_start_yr']
        len_archive = x['archive_end_yr'] - x['archive_start_yr']

        if len_target == len_archive:
            # No problem return the the row as is
            out = x.to_frame().transpose().reset_index(drop=True)
            out = out[['target_start_yr', 'target_end_yr', 'archive_experiment', 'archive_variable',
                       'archive_model', 'archive_ensemble', 'stitching_id', 'archive_start_yr',
                       'archive_end_yr']]

        elif len_target < len_archive:
            # Figure out how much shorter the target period is than the archive period.
            out = x.to_frame().transpose().reset_index(drop=True)
            out = out[['target_start_yr', 'target_end_yr', 'archive_experiment', 'archive_variable',
                       'archive_model', 'archive_ensemble', 'stitching_id', 'archive_start_yr',
                       'archive_end_yr']]
            out['archive_end_yr'] =  out['archive_end_yr'] - 1
        else:
            # TODO need to may need to revisit, just added an extra year to the archive length but that seems somewhat pretty sus.
            # Figure out how much shorter the target period is than the archive period.
            out = x.to_frame().transpose().reset_index(drop=True)
            out = out[['target_start_yr', 'target_end_yr', 'archive_experiment', 'archive_variable',
           'archive_model', 'archive_ensemble', 'stitching_id', 'archive_start_yr',
           'archive_end_yr']]
            out['archive_start_yr'] = out['archive_start_yr'] - 1

        return out

    ser = rp.apply(internal_func, axis=1)
    out = pd.concat(ser.values.tolist()).reset_index(drop=True)

    return out


# def generate_gridded_recipe(messy_recipe, res='mon'):
#     """ Using a messy recipe create the messy recipe that can be used in the
#         stitching process. TODO I think that when the permuate recipe function
#         is fixed, I thinkn that funciton should be nested into here so that this function
#         takes in a matched df & generates the recpies.
#
#
#         :param messy_recipe:       a data frame generated by the permuate_recpies
#         :param res:                string mon or day
#
#         :return:                   a recipe data frame
#     """
#     # Check inputs
#     util.check_columns(messy_recipe, {'target_variable', 'target_experiment', 'target_ensemble',
#                                       'target_model', 'target_start_yr', 'target_end_yr', 'target_year',
#                                       'target_fx', 'target_dx', 'archive_experiment', 'archive_variable',
#                                       'archive_model', 'archive_ensemble', 'archive_start_yr',
#                                       'archive_end_yr', 'archive_year', 'archive_fx', 'archive_dx', 'dist_dx',
#                                       'dist_fx', 'dist_l2', 'stitching_id'})
#     if res in ['mon', 'day'] == False:
#         # TODO figure out a better way to handle the tas resolution matching, will also need
#         # to figure out how to translate this to non tas variables as well.
#         raise TypeError(f"generate_gridded_recipe: does not recognize the res input")
#
#     # Clean up the recipe
#     dat = handle_transition_periods(messy_recipe)
#     dat = handle_final_period(dat)
#
#     # Make sure that if there are historical years of data being used assign
#     # the experiment name to historical.
#     dat.loc[dat['archive_end_yr'] <= 2014, "archive_experiment"] = "historical"
#
#     # Now that we have the formatted recipe add the pangeo tas information!!
#     # TODO remove the dependency on pickles
#     # TODO make sure that that file path is stable
#     pangeo_table = pickle.load("stitches/data/pangeo_table.pkl", compression="zip")
#     if res == 'mon':
#         table = 'Amon'
#     else:
#         table = 'day'
#     tas_meta_info = pangeo_table.loc[(pangeo_table['variable'] == 'tas') &
#                      (pangeo_table['res'] == table)]
#     tas_meta_info = tas_meta_info[['model', 'experiment', 'ensemble', 'variable', 'zstore']]
#     tas_meta_info = tas_meta_info.rename(columns={"model": "archive_model",
#                                               "experiment":"archive_experiment",
#                                               "ensemble":"archive_ensemble",
#                                               "variable":"archive_variable"})
#
#     out = dat.merge(tas_meta_info, how="inner")
#     return out


















