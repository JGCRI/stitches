"""Module for working with the ESGF database through their API.

# https://docs.google.com/document/d/1pxz1Kd3JHfFp8vR2JCVBfApbsHmbUQQstifhGNdc6U0/edit?usp=sharing
# API AT: https://github.com/ESGF/esgf.github.io/wiki/ESGF_Search_REST_API#results-pagination
"""

from __future__ import print_function
import requests
import xml.etree.ElementTree as ET
import pandas as pd
import xarray as xr
import numpy as np


# Author: Unknown
# I got the original version from a word document published by ESGF
# https://docs.google.com/document/d/1pxz1Kd3JHfFp8vR2JCVBfApbsHmbUQQstifhGNdc6U0/edit?usp=sharing
# API AT: https://github.com/ESGF/esgf.github.io/wiki/ESGF_Search_REST_API#results-pagination
def esgf_search(server="https://esgf-node.llnl.gov/esg-search/search",
                files_type="OPENDAP", local_node=True, project="CMIP6",
                verbose=False, format="application%2Fsolr%2Bjson",
                use_csrf=False, **search):
    client = requests.session()
    payload = search
    payload["project"] = project
    payload["type"]= "File"
    if local_node:
        payload["distrib"] = "false"
    if use_csrf:
        client.get(server)
        if 'csrftoken' in client.cookies:
            # Django 1.6 and up
            csrftoken = client.cookies['csrftoken']
        else:
            # older versions
            csrftoken = client.cookies['csrf']
        payload["csrfmiddlewaretoken"] = csrftoken

    payload["format"] = format

    offset = 0
    numFound = 10000
    all_files = []
    files_type = files_type.upper()
    while offset < numFound:
        payload["offset"] = offset
        url_keys = []
        for k in payload:
            url_keys += ["{}={}".format(k, payload[k])]

        url = "{}/?{}".format(server, "&".join(url_keys))
        print(f'\t\t - url: {url}', flush=True)
        r = client.get(url)
        r.raise_for_status()
        resp = r.json()["response"]
        numFound = int(resp["numFound"])
        resp = resp["docs"]
        offset += len(resp)
        for d in resp:
            if verbose:
                for k in d:
                    print("{}: {}".format(k,d[k]))
            url = d["url"]
            for f in d["url"]:
                sp = f.split("|")
                if sp[-1] == files_type:
                    all_files.append(sp[0].split(".html")[0])
    return sorted(all_files)


def get_df_from_esgf(result_df, archive_start_year, archive_end_year, time_chunk = 100 ):
    """
    Download data from an ESGF search using Xarray OpenDAP functionality
    Parameters:
        - result_df: formatted pandas dataframe of results from ESGF search
        - archive_start_year: start of period which must be contained in result
        - archive_end_year: end of period which must be contained in result
    Returns:
        - df: Xarray.DataSet associated to the ESGF search results
    """
    success = False

    for data_node in result_df['node'].unique():
        node_df = result_df[result_df['node'] == data_node]
        min_year = int(np.min(node_df['start'].values))
        max_year = int(np.max(node_df['end'].values))

        # If node contains data that covers our desired period
        if( (min_year <= archive_start_year) & (max_year >= archive_end_year) ):
            # Try downloading
            try:
                print(f'\t\t - Trying to access data from: {data_node}')
                df = xr.open_mfdataset(node_df['dap_link'].values, chunks = {'time':time_chunk})
                success = True
                break
                ...
            except Exception as e:
                print(f'\t\t - failed using node: {data_node}')
                ...
            ...
        ...
    
    if success:
        print(f'\t\t - success using node: {data_node}')
    else:
        raise Exception("Could not find appropriate data using ESGF API")
    
    return df


def format_esgf_result(result):
    """
    Formats and extracts metadata from the list of links returned by the ESGF API search call.

    Parameters:
        - result: list of OpenDAP links resulting from ESGF API search call
    Returns:
        - result_df: formatted pandas dataframe of OpenDAP links + extracted metadata like year info and host data node
    """
    rows = []
    for dap_link in result:
        data_node = dap_link.split('/')[2]
        filename = dap_link.split('/')[-1]
        ensemble_member = filename.split('_')[4]
        year_string = filename.split('.')[0].split('_')[-1]
        start_year = year_string.split('-')[0][0:4]
        end_year = year_string.split('-')[1][0:4]
        row = [data_node, filename, start_year, end_year, dap_link, ensemble_member]
        rows.append(row)
    result_df = pd.DataFrame(rows, columns=['node', 'file', 'start', 'end', 'dap_link', 'ensemble_member'])

    return result_df


def get_recipe_entry_data(row, res='day', variable = 'tas'):
    """
    Downloads the data associated to a given entry in the recipe 

    Parameters:
        - row: pandas dataframe row which IDs the data we're currently interested in
    Returns:
        - df: Xarray.Dataset associated to the given recipe entry
    """

    # Message:
    print(f'\t - Searching ESGF for archive data {row.archive_start_yr}-{row.archive_end_yr} to use as target period {row.target_start_yr}-{row.target_end_yr}', flush=True)

    # Do the ESGF API search
    result = esgf_search(
        table_id=res, variable_id=variable, experiment_id=row.archive_experiment,
        source_id=row.archive_model, member_id=row.archive_ensemble
        )
    
    # Format the results
    result_df = format_esgf_result(result)

    # Download the data, ensuring it contains the required period defined by the recipe
    df = get_df_from_esgf(result_df, row.archive_start_yr, row.archive_end_yr)

    return df