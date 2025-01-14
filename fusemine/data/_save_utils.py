import os
import pickle

import pandas as pd
import polars as pl
import geopandas as gpd

def check_directory_path(path_directory:str, bool_create=True) -> int:
    int_exists = 1

    if not os.path.exists(path_directory):
        if bool_create:
            os.makedirs(path_directory)
        else:
            int_exists = -1

    return int_exists

def save_data(input_data:pl.DataFrame, 
              path_save:str, save_format:str) -> None:
    
    if save_format.lower() in ['pkl', 'pickle']:
        with open(f'{path_save}.pkl', 'wb') as handle:
            pickle.dump(input_data, handle, protocol=pickle.HIGHEST_PROTOCOL)

    elif save_format.lower() == 'csv':
        # TODO: Test
        try:
            print(input_data)
            input_data.write_csv(f'{path_save}.csv')
        except:
            # Incase dataframe consists of some format not supported by polars write csv
            pd_data = input_data.to_pandas()
            pd_data.to_csv(f'{path_save}.csv')

    elif save_format.lower() == 'json':
        # TODO: Create
        file_extension = '.json'

    elif save_format.lower() in ['geojson', 'geo']:
        # TODO: Create
        file_extension = '.geojson'

    else:
        # TODO: Extend list
        raise ValueError("Unsupported save format type",
                         "Currently supported: [CSV, JSON, GeoJSON, Pickle]")