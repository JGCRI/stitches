from stitches.pkgimports import *

###############################################################################

def create_count_table(obj, var_name, experiments, table_ids,
                       min_ensemble_size):
    """ This function counts the number of experiment/ensemble members per model for tas and pr.
    This will help us identify the models we will initially work with for proof of concept.

        :param obj:                 an intake object (eg the pangeo archive generated from intake.open_esm_datastore("https://storage.googleapis.com/cmip6/pangeo-cmip6.json"))
        :param var_name:            the variable name to look up, either tas or pr
        :type var_name:             str
        :param experiments:         list of experiments of interest to query.
        :param table_ids:           list of table ids (e.g. Amon, day) to query.
        :param min_ensemble_size:   the minimum number of acceptable ensemble members.

        :return:                    a data array of the model, experiment count, and the average ensemble members  per experiment.
    """


    # Subset by the experiment and subset the variables.
    query = dict(experiment_id=experiments,
                 table_id=table_ids)
    obj = obj.search(**query)
    subset = obj.search(**dict(variable_id=[var_name])).df[["source_id", "experiment_id", "member_id"]]

    ensemble_count = subset.groupby(["source_id", "experiment_id"]).nunique('member_id')
    ensembles_per_exp = ensemble_count.groupby(["source_id"]).mean("member_id")
    experiment_count = subset[["source_id", "experiment_id"]].groupby(["source_id"]).nunique('experiment_id')

    # join the experiment and ensemble count, but only keep the entries with
    # more than 5 ensemble memebers.
    table = experiment_count.merge(ensembles_per_exp, on="source_id", how="left").reset_index()
    table = table[table["member_id"] > min_ensemble_size]
    table = table.sort_values(by=["experiment_id", "member_id"], ascending=False)
    return table;


###############################################################################

def keep_p1_results(pangeo_query_result):
    """ This function takes in the result of a pangeo query and returns a
    subsetted result that only keeps the p1 experiments.
    """

    df = pangeo_query_result.df.copy()

    df1 = df.loc[(df['member_id'].str.contains('p1') == True)].copy()

    return df1;
