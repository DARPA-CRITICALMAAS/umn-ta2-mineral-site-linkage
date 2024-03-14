import os
from json import dump
import pickle
import regex as re
import polars as pl

def drop_null_attributes(dict_processed_attributes:dict, bool_list:bool) -> dict | list:
    filtered_attribute_data = {key: value for key, value in dict_processed_attributes.items() if value and value!=""}

    if bool_list and filtered_attribute_data:
        return [filtered_attribute_data]
    elif bool_list:
        return {}
    return filtered_attribute_data

def save_mineralsite_output_json(pl_processed_mineralsite, path_directory:str, file_name:str):
    """
    Saves the data in the mineralsite schema format as a json file that can be accepted by the knowledge graph

    : params: df_mineralsite
    : params: file_name
    : params: list_path
    """
    if not os.path.exists(path_directory):
        os.makedirs(path_directory)

    with open(os.path.join(path_directory, file_name+'.pkl'), 'wb') as handle:
        pickle.dump(pl_processed_mineralsite, handle, protocol=pickle.HIGHEST_PROTOCOL)

    # Filter out attributes that are not accepted by the mineral site schema
    mineralsite_attributes = set(['source_id', 'record_id', 'name'])
    locationinfo_attributes = set(['location', 'crs', 'country', 'state_or_province'])
    deposittype_attributes = set(['observed_name'])
    record_attributes = set(list(pl_processed_mineralsite.columns))

    available_mineralsite_attributes = list(record_attributes & mineralsite_attributes)
    available_locationinfo_attributes = list(record_attributes & locationinfo_attributes)
    available_deposittype_attributes = list(record_attributes & deposittype_attributes)

    pl_filtered_mineralsite = pl_processed_mineralsite.select(
        pl.col(available_mineralsite_attributes),
        pl.col(available_locationinfo_attributes),
        pl.col(available_deposittype_attributes),
    ).with_columns(
        pl.all().str.strip_chars()
    )

    print(pl_filtered_mineralsite)

    pl_filtered_mineralsite = pl_filtered_mineralsite.select(
        pl.col(available_mineralsite_attributes),
        location_info = pl.struct(pl.col(available_locationinfo_attributes)),
        deposit_type_candidate = pl.struct(pl.col(available_deposittype_attributes)),
    )

    try:
        # If name attribute is available get the first item in the list
        df_filtered_mineralsite = pl_filtered_mineralsite.with_columns(
            pl.col('name').list.get(0)
        ).to_pandas()
    except:
        df_filtered_mineralsite = pl_filtered_mineralsite.to_pandas()

    df_filtered_mineralsite['location_info'] = df_filtered_mineralsite['location_info'].apply(lambda x: drop_null_attributes(x, False))

    try:
        df_filtered_mineralsite['deposit_type_candidate'] = df_filtered_mineralsite['deposit_type_candidate'].apply(lambda x: drop_null_attributes(x, True))
    except:
        pass

    dict_site_data = df_filtered_mineralsite.to_dict(orient='records')
    list_site_data = [drop_null_attributes(site_data, False) for site_data in dict_site_data]

    dict_site_data = {"MineralSite": list_site_data}

    with open(os.path.join(path_directory, file_name+'.json'), 'w') as f:
        dump(dict_site_data, f, indent=4, default=str)