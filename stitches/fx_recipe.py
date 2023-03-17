# Define the collection of helper functions that are used to generate the different
# permutations of the recipes & re-format for stitching.
import pandas as pd
import pkg_resources
import stitches.fx_util as util
import stitches.fx_match as match


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


def remove_duplicates(md, archive):
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
        :return:                       data frame with same structure as raw matched, with duplicate matches replaced.
    """
    if len(md["target_year"].unique()) < util.nrow(md):
        raise TypeError(f"You have multiple matches to a single target year, this function can only accept a matched "
                        f"data frame of singular matches between target & archive data.")

    #  Initialize everything that gets updated on each iteration of the while loop:
    # 1. the data frame of matched_data -> make a copy of the argument md to initialize
    # 2. the data frame of duplicates is calculated for the first time.
    matched_data = md.copy()

    # Check to see if in the matched data frame if there are any repeated values.
    md_archive = matched_data[['archive_experiment', 'archive_variable', 'archive_model',
                               'archive_ensemble', 'archive_start_yr', 'archive_end_yr',
                               'archive_year', 'archive_fx', 'archive_dx']]
    duplicates = matched_data.merge(md_archive[md_archive.duplicated()], how="inner")

    # As long as duplicates exist, rematch the target windows with the larger
    # dist l2 to each archive chunk, add back in, iterate to be safe.
    # By matching on new_archive = archive - matches that were used in md,
    # we don't introduce new duplicates when we rematch. So the while loop is
    # probably over cautious but it does only execute one iteration.
    while util.nrow(duplicates) > 0:

        # within each iteration of checking duplicates,
        # pull out the one with smallest dist_l2 -
        # this is the one that gets to keep the archive match, and we use
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

        # target points contained in duplicates-duplicates_min 
        # are the  ones that need a new archive match.
        filter_col = [col for col in duplicates if col.startswith('target_')]
        points_to_rematch = duplicates[filter_col].loc[(~duplicates['target_year'].isin(duplicates_min['target_year']))]
        new_names = list(map(lambda x: x.replace('target_', ''), points_to_rematch.columns))
        points_to_rematch.columns = new_names

        # Because we know that none of the archive values can be reused in the match,
        # discard the ones already used (eg in matched_data)
        # from the updated archive that will be used in the rematching.
        cols = [col for col in matched_data if col.startswith('archive_')]
        rm_from_archive = matched_data[cols]
        new_names = list(map(lambda x: x.replace('archive_', ''), rm_from_archive.columns))
        rm_from_archive.columns = new_names

        # Use our anti_join utility function to return the rows of archive that are
        # not in rm_from_archive
        new_archive = util.anti_join(archive, rm_from_archive,
                                     bycols=['model', 'experiment', 'variable', 'ensemble',
                                             'start_yr', 'end_yr', 'year', 'fx', 'dx'])

        # Find new matches for the data the target data that is missing the archive pair. Because we
        # are only interested in completing our singular recipe the tol must be 0.
        rematched = match.match_neighborhood(target_data=points_to_rematch, archive_data=new_archive,
                                             tol=0)

        # Now, we update our key data frames for the next iteration of the while loop:
        # 1. matched_data gets updated to be rematched + (previous matched_data minus the targets
        # that were rematched).
        # 2. duplicates gets recreated, checking for duplicates in our updated matched_data.

        # update matched_data:
        # first, drop the target windows that got rematched from the current matched_data:
        matched_data_minus_rematched_targ_years = matched_data.loc[
            ~(matched_data['target_year'].isin(rematched['target_year']))].copy()

        matched_data = pd.concat([matched_data_minus_rematched_targ_years, rematched]) \
            .sort_values('target_year').reset_index(drop=True)

        # Identify duplicates in the updated matched_data for the next iteration of the while loop
        md_archive = matched_data[['archive_experiment', 'archive_variable', 'archive_model',
                                   'archive_ensemble', 'archive_start_yr', 'archive_end_yr',
                                   'archive_year', 'archive_fx', 'archive_dx']]
        duplicates = matched_data.merge(md_archive[md_archive.duplicated()], how="inner")

        # Clean up for the next while loop iteration
        del (duplicates_min, points_to_rematch, rm_from_archive, rematched,
             matched_data_minus_rematched_targ_years)

    return matched_data


def permute_stitching_recipes(N_matches: int , matched_data, archive, optional=None, testing: bool = False):
    """ A function to sample from input `matched_data` (the the results of `match_neighborhood(target, archive, tol)` to produce permutations  of possible stitching recipes that will match the target data.

            :param N_matches:         a int to the maximum number of matches per target data
            :type N_matches:           int

            :param matched_data:    data output from match_neighborhood.
            :type matched_data:      pd.DataFrame()

            :param archive:         the archive data to use for re-matching duplicate points
            :type archive:          pd.DataFrame()

            :param optional:        a previous output of this function that contains a list of already created recipes to avoid re-making (this is not implemented).

            :param testing:         Boolean True/False. Defaults to False. When True, the behavior can be reliably replicated without setting global seeds.

            :return:                    data frame with same structure as raw matched, with duplicate matches replaced.
        """
    # Check inputs
    util.check_columns(matched_data, {'target_variable', 'target_experiment', 'target_ensemble',
                                      'target_model', 'target_start_yr', 'target_end_yr', 'target_year',
                                      'target_fx', 'target_dx', 'archive_experiment', 'archive_variable',
                                      'archive_model', 'archive_ensemble', 'archive_start_yr',
                                      'archive_end_yr', 'archive_year', 'archive_fx', 'archive_dx', 'dist_dx',
                                      'dist_fx', 'dist_l2'})

    # Initialize quantities updated on every iteration of the while loop:
    # 1. A copy of the matched data
    # 2. perm_guide

    # Initialize matched_data_int for iteration through the while loop:
    # make a copy of the data to work with to be sure we don't touch original argument
    matched_data_int = matched_data.drop_duplicates().copy()

    # identifying how many target windows are in a trajectory we want to
    # create so that we know we have created a full trajectory with no
    # missing windows; basically a reference for us to us in checks.
    num_target_windows = util.nrow(matched_data_int["target_year"].unique())

    # Initialize perm_guide for iteration through the while loop.
    # the permutation guide is one of the factors that the while loop
    # will run checks on, must be initialized.
    # Perm_guide is basically a dataframe where each target window
    # lists the number of archive matches it has.
    num_perms = get_num_perms(matched_data_int)
    perm_guide = num_perms[1]

    # how many target trajectories are we matching to,
    # how many collapse-free ensemble members can each
    # target support, and order them according to that
    # for construction.
    targets = num_perms[0].sort_values(["minNumMatches"]).reset_index()
    # Add a column of a target  id name, differentiate between the different input
    # streams we are emulating.
    # We specifically emulate starting with the realization that can support
    # the fewest collapse-free generated realizations and work in increasing
    # order from there. We iterate over the different realizations to facilitate
    # checking for duplicates across generated realizations across target
    # realizations.
    targets['target_ordered_id'] = ['A' + str(x) for x in targets.index]

    if util.nrow(num_perms[0]["target_experiment"].unique()) > 1:
        raise TypeError(
            f"function permute_stitching_recipes should be applied to separate data frames for each target experiment of interest (multiple target ensemble members for a single target experiment is fine)")

    # max number of permutations per target without repeating across generated
    # ensemble members.
    N_data_max = min(num_perms[0]['minNumMatches'])

    if N_matches > N_data_max:
        print("You have requested more recipes than possible for at least one target trajectories, returning what can")

    # Initialize the number of matches to either 0 or the input read from optional:
    if type(optional) is str:
        print('initialize to the read-in: has not been translated')
    else:
        recipe_collection = pd.DataFrame()

    # Loop over each target ensemble member, creating N_matches generated
    # realizations via a while loop before moving to the next target.
    for target_id in targets['target_ordered_id'].unique():
        # subset the target info, the target df contains meta information about the run we
        # and the number of permutations and such.
        target = targets.loc[targets["target_ordered_id"] == target_id].copy()

        # initialize a recipes data frame holder for each target, for
        # the while loop to iterate on
        recipes_col_by_target = pd.DataFrame()
        var_name = target['target_variable'].unique()[0]
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
        #
        # Initialize these conditions so we enter the while loop, then update again at the
        # end of each iteration:
        if util.nrow(recipes_col_by_target) == 0:
            condition1 = True
        elif util.nrow(recipes_col_by_target['stitching_id'].unique()) < N_matches:
            condition1 = True
        else:
            condition1 = False

        perm_rows = util.nrow(
            perm_guide.loc[(perm_guide['target_variable'] == var_name) & (perm_guide['target_experiment'] == exp) &
                           (perm_guide['target_model'] == mod) & (perm_guide['target_ensemble'] == ens)]
                .copy()
                .drop_duplicates())

        if perm_rows == num_target_windows:
            condition2 = True
        else:
            condition2 = False

        # And an integer index to initialize the count of stitched
        # trajectories for each target
        stitch_ind = 1

        # Run the while loop!
        while all([condition1, condition2]):

            # Group matched data for a single target by the chunks of the target information.
            # Right now a single target chunk may have multiple matches with archive points. The
            # next several steps of the while loop will create a one to one paring between the
            # target and archive data, then check to make sure that the pairing meets the requirements
            # for what we call a recipe.
            grouped_targets = []
            grouped_targets = matched_data_int.loc[(matched_data_int['target_variable'] == var_name) &
                                                   (matched_data_int['target_experiment'] == exp) &
                                                   (matched_data_int['target_model'] == mod) &
                                                   (matched_data_int['target_ensemble'] == ens)].copy().groupby(
                ['target_variable', 'target_experiment', 'target_ensemble', 'target_model',
                 'target_start_yr', 'target_end_yr'])

            # For each target window group,
            # Randomly select one of the archive matches to use.
            # This creates one_one_match, a candidate recipe.
            one_one_match = []
            for name, group in grouped_targets:
                if testing:
                    one_one_match.append(group.sample(1, replace=False, random_state=1))
                else:
                    one_one_match.append(group.sample(1, replace=False))
            one_one_match = pd.concat(one_one_match)
            one_one_match = one_one_match.reset_index(drop=True).copy()

            # Before we can accept our candidate recipe, one_one_match,
            # we run it through a lot of tests.

            # Force one_one_match to meet our first condition,
            # that each archive data point in the recipe must be unique.
            # Then give it a stitching id
            new_recipe = []
            new_recipe = remove_duplicates(one_one_match, archive)
            stitching_id = exp + '~' + ens + '~' + str(stitch_ind)
            new_recipe["stitching_id"] = stitching_id
            new_recipe = new_recipe.reset_index(drop=True).copy()

            # Make sure the new recipe isn't missing any years:
            if ~new_recipe.shape[0] == num_target_windows:
                raise TypeError(f"problem: the new single recipe is missing years of data!")
            #  Make sure that no changes were made to the target years.
            if sum(~new_recipe['target_start_yr'].isin(set(matched_data_int['target_start_yr']))) > 0:
                raise TypeError(f"problem the new single recipe target years!")

            # Compare the new_recipe to the previously drawn recipes across all target
            # ensembles.
            # There is no collapse within each target ensemble because  we remove the constructed
            # new_recipe from the matched_data at the end of each iteration of the while loop -
            # The sampled points CAN'T be used again for the current target ensemble member
            # for loop iteration, or for any other target ensemble members. Meaning we
            # avoid envelope collapse when targeting multiple realizations (you don't have
            # realization 1 and realization 4 2070 getting matched to the same archive point.
            # The code below is checking to make sure that our new_recipe doesn't exist
            # in the saved recipe_collection. This shouldn't be possible with how we update
            # our matched_data_int on every loop, but just to be cautious, we check.
            # Again, the challenge is seeing if our entire sample has
            # been included in recipes before, not just a row or two.

            if util.nrow(recipe_collection) != 0:
                # If previous recipes exist, we must create a comparison
                # data frame that checks each existing recipe in recipe_collection
                # against new_recipe and record True/False
                #
                # Compare the new recipe with the existing collection of all recipes.
                cols_to_use = ['target_variable', 'target_experiment',
                               'target_model', 'target_start_yr', 'target_end_yr', 'archive_experiment',
                               'archive_variable', 'archive_model', 'archive_ensemble', 'archive_start_yr',
                               'archive_end_yr']
                grouped_collection = recipe_collection.groupby(['stitching_id'])
                comparison = []
                for name, group in grouped_collection:
                    df1 = group[cols_to_use].copy().reset_index(drop=True).sort_index(axis=1)
                    df2 = new_recipe[cols_to_use].copy().reset_index(drop=True).sort_index(axis=1)
                    comparison.append(df1.equals(df2))

                # end for loop
            # end if statement
            else:
                # Otherwise, this is the first recipe we've done at all, so we set comparison manually
                # so that the next if statement triggers just like it was appending a new recipe to an
                # existing list.
                comparison = [False]
            # end else

            # If the new_recipe is not unique (aka, any(comparison) == True), then
            # we don't want it and we don't want to do anything else in this iteration of
            # the while loop. We DON'T update the matched_points or conditions, so the
            # while loop is forced to re-run so that another random draw is done to create
            # a new candidate new_recipe.
            # We check for what we want: the new recipe is the first or all(comparison)==False.
            # In either case, we are safe to keep new_recipe and update all the data frames
            # for the next iteration of the while loop.
            if all(comparison) == False:

                # add new_recipe to the list of recipes for this target ensemble
                recipes_col_by_target = pd.concat([recipes_col_by_target, new_recipe])

                # And we remove it from the matched_points_int so the archive
                # values used in this new_recipe can't be used to construct
                # subsequent realizations for this target ensemble member.
                # This updated matched_data_int is used in each iteration
                # of the while loop. Since we are removing the constructed
                # new_recipe from the matched_data_int at the end of each
                # iteration of the while loop, the sample points can't be
                # randomly drawn again for the next generated trajectory
                # of the current target ensemble member for loop iteration.

                # Now each (target_window, archive_window) combination must
                # be removed from matched data for all target ensemble members,
                # not just the one we are currently operating on.
                # This ensures that we don't get collapse in the generated
                # envelope across target ensemble members (e.g you don't
                # have realization 1 and realization 4 2070 getting matched
                # to the same archive point).
                # Use an anti-join
                matched_data_int = util.anti_join(matched_data_int, new_recipe.drop(['stitching_id'], axis=1).copy(),
                                                  bycols=["target_year", "target_start_yr", "target_end_yr",
                                                          "archive_experiment", "archive_variable", "archive_model",
                                                          "archive_ensemble", "archive_start_yr", "archive_end_yr",
                                                          "archive_year"]).copy()

                # update permutation count info with the revised matched data so
                # the while loop behaves - this makes sure that every target window
                # in the perm_guide actually has at least one matched archive point
                # available for draws .
                # That way, we don't try to construct a trajectory with fewer years
                # than the targets.
                num_perms = get_num_perms(matched_data_int)
                perm_guide = num_perms[1]

                # Use the updated perm_guide to update
                # the while loop conditions:

                # Condition 1:
                # If we haven't reached the N_matches goal for this target ensemble
                if util.nrow(recipes_col_by_target) == 0:
                    condition1 = True
                elif util.nrow(recipes_col_by_target['stitching_id'].unique()) < N_matches:
                    condition1 = True
                else:
                    condition1 = False
                # end updating Condition 1

                # Condition 2:
                # make sure each target window in the updated perm guide has at least one archive match available
                # to draw on the next iteration.
                perm_rows = util.nrow(
                    perm_guide.loc[
                        (perm_guide['target_variable'] == var_name) & (perm_guide['target_experiment'] == exp) &
                        (perm_guide['target_model'] == mod) & (perm_guide['target_ensemble'] == ens)]
                        .copy()
                        .drop_duplicates())

                if perm_rows == num_target_windows:
                    condition2 = True
                else:
                    condition2 = False
                # end updating condition 2

                # Add to the stitch_ind, to update the count of stitched
                # trajectories for each target ensemble member.
                stitch_ind += 1

            # end if statement
        # end the while loop for this target ensemble member

        # Add the collection of the recipes for each of the targets into single df.
        recipe_collection = pd.concat([recipe_collection, recipes_col_by_target]).reset_index(drop=True).copy()

    # end the for loop over target ensemble members

    # do outputs
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
        # First check to see the archive period spans the historical to future scenario
        transition_period = (
                (2014 in range(x["archive_start_yr"], x["archive_end_yr"] + 1)) &
                (2015 in range(x["archive_start_yr"], x["archive_end_yr"] + 1))
        )

        if transition_period:
            target_yrs = list(range(x['target_start_yr'], x['target_end_yr']+1))
            archive_yrs = list(range(x['archive_start_yr'], x['archive_end_yr']+1))
            hist_cut_off = 2014  # the final complete year of the historical experiment

            historical_yrs = list(filter(lambda x: x <= hist_cut_off, archive_yrs))
            future_yrs = list(set(archive_yrs).difference(set(historical_yrs)))

            # This is the information that is constant between the historical and future periods.
            constant_info = x.loc[x.index.isin({'archive_variable', 'archive_model',
                                                'archive_ensemble', 'stitching_id'})]

            # Construct the historical period information
            d = {'target_start_yr': min(target_yrs),
                 'target_end_yr': target_yrs[len(historical_yrs)-1],
                 'archive_experiment': 'historical',
                 'archive_start_yr': min(historical_yrs),
                 'archive_end_yr': max(historical_yrs)}
            ser = pd.Series(data=d, index=['target_start_yr', 'target_end_yr', 'archive_experiment',
                                           'archive_start_yr', 'archive_end_yr'])
            historical_period = pd.concat([ser, constant_info]).to_frame().transpose()


            # Check to make sure the lengths of time are correct
            targ_len = historical_period['target_end_yr'].values - historical_period['target_start_yr'].values
            arch_len = historical_period['archive_end_yr'].values - historical_period['archive_start_yr'].values
            if targ_len != arch_len:
                raise TypeError(f"problem with the length of the historical archive & target yrs")

            # Now construct the future period information
            d = {'target_start_yr': target_yrs[len(historical_yrs)],
                 'target_end_yr': target_yrs[len(target_yrs) - 1],
                 'archive_experiment': x['archive_experiment'],
                 'archive_start_yr': min(future_yrs),
                 'archive_end_yr': max(future_yrs)}
            ser = pd.Series(data=d, index=['target_start_yr', 'target_end_yr', 'archive_experiment',
                                           'archive_start_yr', 'archive_end_yr'])
            # future_period = ser.append(constant_info).to_frame().transpose()
            future_period = pd.concat([ser, constant_info]).to_frame().transpose()

            # Check to make sure the lengths of time are correct
            targ_len = future_period['target_end_yr'].values - future_period['target_start_yr'].values
            arch_len = future_period['archive_end_yr'].values - future_period['archive_start_yr'].values
            if not targ_len == arch_len:
                raise TypeError(f"problem with the length of the historical archive & target yrs")


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
    of the archive matches. Since the final period is typically only one year shorter than the
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
            out['archive_end_yr'] = out['archive_end_yr'] - 1
        else:
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


def generate_gridded_recipe(messy_recipe, res: str = 'mon'):
    """ Using a messy recipe create the recipe that can be used in the stitching process.

         :param messy_recipe:       a data frame generated by the permute_recipes
         :type messy_recipe:         pd.DataFrame()
         
         :param res:                string mon or day
         :type res:                 str

         :return:                   a recipe data frame
     """
    # Check inputs
    util.check_columns(messy_recipe, {'target_variable', 'target_experiment', 'target_ensemble',
                                      'target_model', 'target_start_yr', 'target_end_yr', 'target_year',
                                      'target_fx', 'target_dx', 'archive_experiment', 'archive_variable',
                                      'archive_model', 'archive_ensemble', 'archive_start_yr',
                                      'archive_end_yr', 'archive_year', 'archive_fx', 'archive_dx', 'dist_dx',
                                      'dist_fx', 'dist_l2', 'stitching_id'})
    if res not in ['mon', 'day']:
        raise TypeError(f"generate_gridded_recipe: does not recognize the res input")

    # Clean up the recipe
    dat = handle_transition_periods(messy_recipe)
    dat = handle_final_period(dat)

    # Make sure that if there are historical years of data being used assign
    # the experiment name to historical.
    dat.loc[dat['archive_end_yr'] <= 2014, "archive_experiment"] = "historical"

    # Now that we have the formatted recipe add the pangeo tas information!!
    ptable_path = pkg_resources.resource_filename('stitches', 'data/pangeo_table.csv')
    pangeo_table = pd.read_csv(ptable_path)

    if res == 'mon':
        table = 'Amon'
    else:
        table = 'day'
    tas_meta_info = pangeo_table.loc[(pangeo_table['variable'] == 'tas') &
                                     (pangeo_table['domain'] == table)]
    tas_meta_info = tas_meta_info[['model', 'experiment', 'ensemble', 'variable', 'zstore']]
    tas_meta_info = tas_meta_info.rename(columns={"model": "archive_model",
                                                  "experiment": "archive_experiment",
                                                  "ensemble": "archive_ensemble",
                                                  "variable": "archive_variable"})

    out = dat.merge(tas_meta_info, how="inner", on=['archive_model', 'archive_experiment',
                                                      'archive_ensemble', 'archive_variable'])
    out = out.reset_index(drop=True).copy()
    out = out.sort_values(['stitching_id', 'target_start_yr', 'target_end_yr']).copy()
    out = out.reset_index(drop=True).copy()

    # Make sure that no information has been lost, if that is the case raise an error.
    if util.nrow(out) != util.nrow(dat):
        raise TypeError(f"Problem in generate_gridded_recipe, loosing data in recipe!")

    return out


def make_recipe(target_data, archive_data, N_matches: int, res: str = "mon",
                tol: float = 0.1, non_tas_variables: [str] = None,
                reproducible: bool = False):
    """ Generate a stitching recipe from target and archive data.

         :param target_data:       a pandas data frame of climate information to emulate.
         :type target_data:         pd.DataFrame()

         :param archive_data:      a pandas data frame of temperature data to use as the archive to match on.
         :type archive_data:        pd.DataFrame()

         :param N_matches:         a int to the maximum number of matches per target data
         :type N_matches:           int

         :param res:               str of 'mon' or 'day' to indicate the resolution of the stitched data
         :type res:                 str

         :param tol:               float value indicating the tolerance to use in the matching process, default set to 0.1
         :type tol:                 float

         :param non_tas_variables: a list of variables other than tas to stitch together, when using the default set to None only tas will be stitched together.
         :type non_tas_variables: [str]

         :param reproducible:         Boolean True/False. Defaults to False. If True, the call to permute_stitching_recipes()
                                                uses the testing=True argument so that the behavior can be reliably replicated without setting global seeds.

         :return:                   pandas data frame of a formatted recipe
     """

    # Check the inputs
    util.check_columns(target_data, set(['experiment', 'variable', 'ensemble', 'model', 'start_yr',
                                         'end_yr', 'year', 'fx', 'dx']))
    util.check_columns(archive_data, set(['experiment', 'variable', 'ensemble', 'model', 'start_yr',
                                          'end_yr', 'year', 'fx', 'dx']))
    if not type(N_matches) is int:
        raise TypeError(f"N_matches: must be an integer")
    if not type(tol) is float:
        raise TypeError(f"tol: must be a float")


    # If there are non tas variables to be stitched, subset the archive to limit
    # the coverage to only the entries with the complete coverage.
    if type(non_tas_variables) == list:
        if res not in ['mon', 'day']:
            raise TypeError(f"does not recognize the res input")
        if 'tas' in non_tas_variables:
            raise TypeError(f"non_tas_variables: cannot contain tas")

        pt_path = pkg_resources.resource_filename('stitches', 'data/pangeo_table.csv')
        pangeo_table = pd.read_csv(pt_path)

        var_list = pangeo_table["variable"].unique()
        if not set(non_tas_variables) <= set(var_list):
            raise TypeError(f"1 or more of the variables are not found in pangeo table.")

        # Subset the pangeo table so that it contains the resolution & variables of data of interest.
        non_tas_variables.append('tas')
        pt_subset = pangeo_table.loc[(pangeo_table["domain"].str.contains(res) &
                                      pangeo_table["variable"].isin(non_tas_variables) &
                                      ~pangeo_table["zstore"].str.contains("AerChemMIP"))].copy()

        # Format the variable column to the "file" so that we can
        pt_subset["variable"] = pt_subset["variable"] + "_file"
        wide_df = pt_subset.pivot(index=["model", "ensemble", "experiment"], columns="variable", values="zstore")
        wide_df.reset_index(inplace=True)
        wide_df.columns.name = None
        wide_df = wide_df.dropna()

        # Select the archive entries for the model, ensemble, experiment to keep, there are the entries
        # that also have complete coverage for the variables listed in the non tas variable list.
        to_keep = wide_df.loc[:, wide_df.columns.isin(["model", "ensemble", "experiment"])].drop_duplicates()
        archive_data = archive_data.merge(to_keep, on=["model", "ensemble", "experiment"], how="inner").copy()

    # Match the archive & target data together.
    match_df = match.match_neighborhood(target_data, archive_data, tol=tol)


    if reproducible:
        unformatted_recipe = permute_stitching_recipes(N_matches=N_matches,
                                                       matched_data=match_df,
                                                       archive=archive_data,
                                                       testing=True)
    else:
        unformatted_recipe = permute_stitching_recipes(N_matches=N_matches,
                                                       matched_data=match_df,
                                                       archive=archive_data,
                                                       testing=False)

    # Format the recipe into the dataframe that can be used by the stitching functions.
    recipe = generate_gridded_recipe(unformatted_recipe, res=res)
    recipe.columns = ['target_start_yr', 'target_end_yr', 'archive_experiment', 'archive_variable',
                      'archive_model', 'archive_ensemble', 'stitching_id', 'archive_start_yr',
                      'archive_end_yr', 'tas_file']

    # If there are non tas variables add the non tas variables to the formatted recpie.
    if type(non_tas_variables) == list:

        to_join = wide_df.loc[:, ~wide_df.columns.isin(["model", "ensemble", "experiment"])]
        out = pd.merge(recipe, to_join, on='tas_file')

    else:
        out = recipe.copy()

    out = out.sort_values(by=['stitching_id', 'target_start_yr']).reset_index(drop=True).copy()
    return out


