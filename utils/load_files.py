import os
import logging

import pickle
import pandas as pd
import polars as pl
import polars.selectors as cs
import geopandas as gpd

def as_dictionary(input_data, key_column:str, value_column: str) -> dict:
    return dict(zip(input_data[key_column], input_data[value_column]))

def initiate_load(input_filename: str, bool_asdict=False, key_column=None, value_column=None, list_target_filename:str|None=None):
    file_basename = os.path.splitext(os.path.basename(input_filename))[0]
    file_name, file_extension = os.path.splitext(input_filename)

    if list_target_filename and file_basename not in list_target_filename:
        return pl.DataFrame()

    input_data = pl.DataFrame()

    match file_extension:
        case '.csv':
            input_data = pl.read_csv(input_filename, ignore_errors=True)

        case '.gdb':
            input_data = gpd.read_file(
                input_filename, 
                driver="OpenFileGDB"
            )

        case '.pkl':
            with open(input_filename, 'rb') as handle:
                input_data = pickle.load(handle)

        case '.json':
            input_data = pl.read_json(input_filename)

        case _:
            # input_data = initiate_load(input_filename+'dictionary.csv', bool_asdict=True, key_column='label', value_column='definition')
            pass

    if bool_asdict:
        return as_dictionary(input_data, key_column, value_column)

    return input_data

def custom_aggregate(input_list:list):
    # filtered_list = list(filter(lambda item: item is not None, input_list))

    # if len(filtered_list) < 2:
    #     return filtered_list

    # if isinstance(filtered_list[0], str):
    #     print(",".join(filtered_list))
    #     return ",".join(filtered_list)
    
    # else:
    #     return filtered_list[0]
    try:
        return ','.join(input_list)
    except:
        try:
            return input_list[0]
        except:
            return input_list

def load_directory(input_directory:str, list_target_filename:list|None=None, group_by_col:str|None=None):
    files_in_directory = os.listdir(input_directory)

    list_data = []
    for file in files_in_directory:
        loaded_data = initiate_load(os.path.join(input_directory, file), list_target_filename=list_target_filename)
        
        if not loaded_data.is_empty():
            list_data.append(loaded_data)

    pl_data = pl.concat(
        list_data,
        how='align'
    )

    if group_by_col:
        pl_data = pl_data.group_by(
            group_by_col
        ).agg(
            [pl.all()]
        ).with_columns(
            pl.exclude(group_by_col).list.unique().list.drop_nulls().map_elements(lambda x: custom_aggregate(x))
        )

    return pl_data