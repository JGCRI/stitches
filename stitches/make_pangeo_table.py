# Save a copy of the pangeo table, this has information about the z store file.
import pandas as pd

import stitches.fx_pangeo as pangeo
import stitches.fx_util as util
import pandas as pd
import pkg_resources

def make_pangeo_table():
    """"
    The function that makes a copy of the files that are available on pangeo that have corresponding files
    in the the matching archive, this will be used in the stitching process.
    :return:          Nothing, write a file out to package data.
    """
    # Using the information about what experiment/ensemble/models that are avaiable for matching.
    archive_path = pkg_resources.resource_filename('stitches', 'data/matching_archive.csv')
    tas_exp_model = (pd.read_csv(archive_path)[["experiment", "ensemble", "model"]]
                     .drop_duplicates()
                     .reset_index(drop=True)
                     )

    # For the future experiments make sure we add in the historical experiment info.
    fut_exps = ['ssp126', 'ssp245', 'ssp370', 'ssp585', 'ssp534-over']
    hist_info = tas_exp_model.loc[tas_exp_model["experiment"].isin(fut_exps)].copy()[['model', 'ensemble']]
    hist_info["experiment"] = "historical"
    hist_info = hist_info.drop_duplicates()
    to_keep = pd.concat([tas_exp_model, hist_info], axis=0)
    to_keep = to_keep.reset_index(drop=True)

    # Get the complete data table from pangeo
    dat = pangeo.fetch_pangeo_table()
    pangeo_table = (dat.loc[dat['grid_label'] == "gn"][["source_id", "experiment_id", "member_id", "variable_id",
                                                        "zstore", "table_id"]].copy())
    pangeo_table = pangeo_table.rename(columns={"source_id": "model", "experiment_id": "experiment",
                                                "member_id": "ensemble", "variable_id": "variable",
                                                "zstore": "zstore", "table_id": "domain"})
    pangeo_table = pangeo_table.reset_index(drop=True)

    # Subset the pangeo table so that it only contains information for what we have deemed as good data.
    final_pangeo_table = pangeo_table.merge(to_keep, how="inner", on=['model', 'experiment', 'ensemble'])

    # Write the file out
    out_file = pkg_resources.resource_filename('stitches', 'data') + '/pangeo_table.csv'
    final_pangeo_table.to_csv(out_file, index=False)

    # Return
    return None

def make_pangeo_comparison():
    """"
    The function that makes a copy of the entire pangeo archive. This will be used in
    testing to check to see if there has been an update to the pangeo archive, if there
    is then may suggest updating the internal package data.
    :return:          Nothing, write a file out to package data.
    """

    dat = pangeo.fetch_pangeo_table()
    out_file = pkg_resources.resource_filename('stitches', 'data') + '/pangeo_comparison_table.csv'
    dat.to_csv(out_file, index=False)
    # Return
    return None