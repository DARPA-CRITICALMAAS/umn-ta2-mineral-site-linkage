import os
import logging

import pickle
import regex as re
import pandas as pd
import polars as pl
import polars.selectors as cs
import geopandas as gpd

def as_dictionary(input_data, key_column:str, value_column: str) -> dict:
    return dict(zip(input_data[key_column], input_data[value_column]))

def initiate_load(input_filename: str, bool_asdict=False, key_column=None, value_column=None, list_target_filename:str|None=None, list_exclude_filename:list|None=None):
    file_basename = os.path.splitext(os.path.basename(input_filename))[0]
    file_name, file_extension = os.path.splitext(input_filename)

    if (list_target_filename and file_basename not in list_target_filename) or (list_exclude_filename and file_basename in list_exclude_filename):
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

        case '.txt':
            input_data = pd.read_csv(input_filename,
                                     header=0,
                                     delimiter='\t')
            input_data = pl.from_pandas(input_data)

        case _:
            # input_data = initiate_load(input_filename+'dictionary.csv', bool_asdict=True, key_column='label', value_column='definition')
            pass

    if bool_asdict:
        return as_dictionary(input_data, key_column, value_column)

    return input_data

def custom_aggregate(input_list:list):
    try:
        return ','.join(input_list)
    except:
        try:
            return input_list[0]
        except:
            return input_list

def load_directory(input_directory:str, list_target_filename:list|None=None, list_exclude_filename:list|None=None, group_by_col:str|None=None):
    files_in_directory = os.listdir(input_directory)

    pl_data = pl.DataFrame()
    for file in files_in_directory:
        loaded_data = initiate_load(os.path.join(input_directory, file), list_target_filename=list_target_filename)
        
        if not loaded_data.is_empty():
            if pl_data.is_empty():
                pl_data = loaded_data

            else:
                try:
                    pl_data = pl.concat(
                        [pl_data, loaded_data],
                        how='align'
                    )
                except:
                    pass

    if group_by_col:
        pl_data = pl_data.group_by(
            group_by_col
        ).agg(
            [pl.all()]
        ).with_columns(
            pl.exclude(group_by_col).list.unique().list.drop_nulls().map_elements(lambda x: custom_aggregate(x))
        )

    return pl_data

def load_directory_names(raw_data_path:str):
    items_in_directory = os.listdir(raw_data_path)
    data_directories = {}

    for i in items_in_directory:
        directory_path = os.path.join(raw_data_path, i)

        cleaned_items = re.sub('[^A-Za-z0-9]', '',  i).lower()
        data_directories[cleaned_items] = directory_path
   
    return data_directories