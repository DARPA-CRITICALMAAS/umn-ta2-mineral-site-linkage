import os
from json import dump

import polars as pl

def save_mineralsite_output_json(df_site_data, file_name:str, list_path=[]):
    """
    Saves the data in the mineralsite schema format as a json file that can be accepted by the knowledge graph

    : params: df_site_data
    : params: file_name
    : params: list_path
    """
    path_dir = ''

    for i in list_path:
        path_dir = os.path.join(path_dir, i)
    if not os.path.exists(path_dir):
        os.makedirs(path_dir)

    dict_site_data = df_site_data.to_dict(orient='records')
    dict_site_data = {"MineralSite":dict_site_data}

    with open(os.path.join(path_dir, file_name+'.json'), 'w') as f:
        dump(dict_site_data, f, indent=4, default=str)