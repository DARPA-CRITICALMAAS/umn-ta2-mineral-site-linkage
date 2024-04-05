import os
import logging

import pickle
import pandas as pd
import polars as pl
import geopandas as gpd

# def as_dataframe(input_data: str):
#     return 0

def as_dictionary(input_data, key_column:str, value_column: str) -> dict:
    return dict(zip(input_data[key_column], input_data[value_column]))

def initiate_load(input_filename: str, bool_asdict=False, key_column=None, value_column=None, list_target_filename:str|None=None):
    file_name, file_extension = os.path.splitext(input_filename)

    if list_target_filename and file_name not in list_target_filename:
        return None

    match file_extension:
        case '.csv':
            input_data = pl.read_csv(input_filename)

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
            input_data = initiate_load(input_filename+'dictionary.csv', bool_asdict=True, key_column='label', value_column='definition')

    if bool_asdict:
        return as_dictionary(input_data, key_column, value_column)

    return input_data

def load_directory(input_directory:str, list_target_filename:list|None=None):
    files_in_directory = os.listdir(input_directory)

    loaded_files = {}

    for file in files_in_directory:
        loaded_data = initiate_load(os.path.join(input_directory, file), list_target_filename=list_target_filename)

        if loaded_data:
            loaded_files[file] = loaded_data

    return loaded_files