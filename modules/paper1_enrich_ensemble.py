# x experiments
# Slack message 2021-10-21
# Hi @abigail.snyder. Would it be possible to set up a couple of experiments taking two of the models
# that have ~8/~10 ensemble members per scenario (hoping for at least three scenarios with such richness
# of IC members), targeting the central scenario, and producing as many new members for it as it is possible
# to obtain, possibly exploring a range (3?) of tolerance values, and sampling from the members under
# the target scenario itself as well (but not only, i.e. using the other scenarios’ members available as well)?
# tolerances of (0.07, 0.1, 0.13)?


import stitches
import pandas as pd
import pkg_resources
import random

# Set the seed so that it is reproducible
random.seed(10)

# Start by figuring out which of the models meets the criteria ~8/~10 ensemble members per scenario
# pangeo table of ESMs for reference
archive_path = pkg_resources.resource_filename('stitches', 'data/matching_archive.csv')
archive_data = pd.read_csv(archive_path)
archive_data = archive_data[archive_data['experiment'].isin({'ssp126', 'ssp245', 'ssp370', 'ssp585',
                                         'ssp119',  'ssp534-over', 'ssp434', 'ssp460'})].copy()


pangeo_data = archive_data[["experiment", "ensemble", "model"]].drop_duplicates()

# Subset the pangeo data frame so that we only have data for the experiments we are interested in.
exps = ["ssp126", "ssp245", "ssp585", "ssp370"]

# Subset the pangeo data so that it only include data for the models that have the experiments of
# interest.
experiment_count = (pangeo_data.loc[pangeo_data["experiment"].isin(exps)].copy()
                    [['experiment', 'model']]
                    .drop_duplicates()
                    .groupby(['model'])
                    .agg({"experiment": "size"})
                    .reset_index()
                    )
models_to_keep = experiment_count.loc[experiment_count["experiment"] == len(exps)]["model"]

# Count the ensembles we are looking for a handful of models that have about ~8 to ~10 members per experiment.
ensemble_count = (
    pangeo_data.loc[(pangeo_data["experiment"].isin(exps)) & (pangeo_data["model"].isin(models_to_keep))].copy()
    [['experiment', 'model', 'ensemble']]
        .drop_duplicates()
        .groupby(['model', 'experiment'])
        .agg({"ensemble": "nunique"})
        .reset_index()
)

# Read in the data that will be used as the archive & the target.
archive_path = pkg_resources.resource_filename('stitches', 'data/matching_archive.csv')
archive_data = pd.read_csv(archive_path)

# limiting to a specific model...
limit_to_this = 'CanESM5' # 'ACCESS-ESM1-5' # , 'MIROC-ES2L']
archive_data = archive_data.loc[archive_data["model"] == limit_to_this]
ensemble_count = ensemble_count.loc[ensemble_count["model"] == limit_to_this]

# Determine the ensemble sizes
ensemble_size = [5, 10]
for en_size in ensemble_size:

    sufficient_en_count = ensemble_count.loc[ensemble_count["ensemble"] >= en_size].copy()
    exp_count = (sufficient_en_count.groupby(['model'])
                 .agg({"experiment": "size"})
                 .reset_index())

    # Determine which models meet the requirement for all 4 experiments.
    model_list = exp_count.loc[exp_count["experiment"] == 4]["model"]

    # Iterate through each model that meets the experiment requirements
    for model in model_list:
        for target_exp in ["ssp245", "ssp370"]:

            # Set up the base name
            base_file_name = pkg_resources.resource_filename('stitches', '/enrich_ensemble_size/') + str(en_size) + "_" + model + "_" + target_exp

            # Determine which experiments to keep use in the archive then separate the
            # data into the archive and target data sets.
            archive_exps = set(exps)
            archive_to_use = archive_data.loc[(archive_data["model"] == model) & (archive_data["experiment"].isin(archive_exps))]
            target_to_use = archive_data.loc[(archive_data["model"] == model) & (archive_data["experiment"] == target_exp)]

            # TODO might need to add an assert function to make sure that there is actual data.

            # Subset the archive so that is only contains as many ensemble members as en_size.
            selected_en = random.sample(set(target_to_use.ensemble.unique()), en_size)
            archive_to_use = archive_to_use.loc[archive_to_use["ensemble"].isin(selected_en)].copy()

            # Save a copy of the archive & target.
            fname = base_file_name + "_archive.csv"
            archive_to_use.to_csv(fname)
            fname = base_file_name + "_target.csv"
            target_to_use.to_csv(fname)

            # Read in & save the comparison data.
            data_path = pkg_resources.resource_filename('stitches', 'data/tas-data/' + model + "_tas.csv")
            data = pd.read_csv(data_path)
            data = data.loc[(data["experiment"] == target_exp) & (data["ensemble"].isin(selected_en))].copy()
            fname = base_file_name + "_compdata.csv"
            data.to_csv(fname)

            # Generate the recipes using the various tols.
            N_matches = int(1e100)
            print(".07")
            rp_07 = stitches.make_recipe(target_data=target_to_use, archive_data=archive_to_use, N_matches=N_matches, tol=0.07)
            print(".1")
            rp_1 = stitches.make_recipe(target_data=target_to_use, archive_data=archive_to_use, N_matches=N_matches, tol=0.1)
            print(".13")
            rp_13 = stitches.make_recipe(target_data=target_to_use, archive_data=archive_to_use, N_matches=N_matches, tol=0.13)

            rp_07["tol"] = "0.07"
            rp_1["tol"] = "0.10"
            rp_13["tol"] = "0.13"

            out = pd.concat([rp_07, rp_1, rp_13])
            fname = base_file_name + "_recepie.csv"
            out.to_csv(fname)

            out_07 = stitches.gmat_stitching(rp_07)
            out_1 = stitches.gmat_stitching(rp_1)
            out_13 = stitches.gmat_stitching(rp_13)

            out_07["tol"] = "0.07"
            out_1["tol"] = "0.10"
            out_13["tol"] = "0.13"

            out = pd.concat([out_07, out_1, out_13])
            out["experiment"] = target_exp
            out["ensemble_size"] = en_size
            fname = base_file_name + "_synthetic.csv"
            out.to_csv(fname)
        # End of target for loop
    # End of model for loop
