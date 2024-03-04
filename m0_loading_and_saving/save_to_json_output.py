import os
from json import dump

import polars as pl

def save_mineralsite_output_json(pl_processed_mineralsite, path_directory:str, file_name:str):
    """
    Saves the data in the mineralsite schema format as a json file that can be accepted by the knowledge graph

    : params: df_mineralsite
    : params: file_name
    : params: list_path
    """
    if not os.path.exists(path_directory):
        os.makedirs(path_directory)

    # Filter out attributes that are not accepted by the mineral site schema
    mineralsite_attributes = set(['source_id', 'record_id', 'name'])
    locationinfo_attributes = set(['location_info', 'crs', 'country', 'state_or_province'])
    record_attributes = set(list(pl_processed_mineralsite.columns))

    available_mineralsite_attributes = list(record_attributes & mineralsite_attributes)
    available_locationinfo_attributes = list(record_attributes & locationinfo_attributes)

    pl_filtered_mineralsite = pl_processed_mineralsite.select(
        pl.col(available_mineralsite_attributes),
        pl.struct(pl.col(available_locationinfo_attributes)),
    )
    
    try:
        # If name attribute is available get the first item in the list
        pl_filtered_mineralsite = pl_filtered_mineralsite.with_columns(
            pl.col('name').list.get(0)
        ).to_pandas()
    except:
        df_filtered_mineralsite = pl_filtered_mineralsite.to_pandas()

    dict_site_data = df_filtered_mineralsite.to_dict(orient='records')
    dict_site_data = {"MineralSite": dict_site_data}

    with open(os.path.join(path_directory, file_name+'.json'), 'w') as f:
        dump(dict_site_data, f, indent=4, default=str)