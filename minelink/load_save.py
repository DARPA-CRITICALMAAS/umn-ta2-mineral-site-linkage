#!/usr/bin/python
import os
import sys
import shutil

import pickle5 as pickle
from json import loads, dumps

import pandas as pd
import geopandas as gpd
from shapely import wkt
# from transformers import AutoTokenizer, AutoModel

from minelink.params import *

def check_dir(path_dir):
    if not os.path.exists(path_dir):
        os.makedirs(path_dir)

def loader(path_dir, file_name, extension):
    if extension == '.gdb':
        return gpd.read_file(os.path.join(path_dir, file_name + extension), driver="OpenFileGDB")
    
    elif extension == '.geojson':
        return gpd.read_file(os.path.join(path_dir, file_name + extension))
    
    elif extension == '.pkl':
        with open(os.path.join(path_dir, file_name+'.pkl'), 'rb') as handle:
            dataframe = pickle.load(handle)
        return dataframe
    
    elif extension == '.csv':
        return(pd.read_csv(os.path.join(path_dir, file_name + extension)))
    
    # TODO: Somethng is wrong here; it needs to be editted so that later user inputs can also be read
    # elif extension == '.json':
    #     f = open(os.path.join(path_dir, file_name + extension), 'r')
    #     data  = loads(f)
    #     return data

def dumper(data, path_dir, file_name, save_format):
    if save_format.upper() == 'PKL' or save_format.upper() == 'PICKLE':
        with open(os.path.join(path_dir, file_name+'.pkl'), 'wb') as handle:
            pickle.dump(data, handle, protocol=pickle.HIGHEST_PROTOCOL)

    elif save_format.upper() == 'CSV':
        data.to_csv(os.path.join(path_dir, file_name + '.csv'))
    
    elif save_format.upper() == 'GEOJSON':
        return 0
    
    elif save_format.upper() == 'JSON':
        json_df = data.to_json(orient='index', default_handler=str)    # default handler set to prevent iteration overflow

        json_data = loads(json_df)
        obj_data = dumps(json_data, indent=4)

        with open(os.path.join(path_dir, file_name+'.json'), 'w') as handle:
            handle.write(obj_data)

def load_dir_items(path_dir):
    list_dir_items = os.listdir(path_dir)
    list_files = ['',]
    
    bool_confirm = False
    while not bool_confirm:
        for l in list_dir_items:
            file_name, file_extension = os.path.splitext(l)
            if file_extension == '':
                pass
            elif file_extension in ACCEPTABLE_INPUT_FILE:
                list_files.append(l)

        print("\nModel running with the following files: " + "\n|-- ".join(list_files))

        if len(list_files) == 1:
            break

        user_confirm = str(input("> Confirmed (Y/N)?: ")).upper()
        if user_confirm == 'Y':
            bool_confirm = True
        elif user_confirm == 'N':
            user_remove = str(input("> Which file should be removed (Enter the exact name, case sensitive)?: "))
            list_files.remove(user_remove)

            list_dir_items = list_files
            list_files = ['',]

    # TODO: Do something that will print out {filename} AS {extension}
        
    folder_temporary = './temporary'
    check_dir(folder_temporary)

    for l in list_files:
        file_name, file_extension = os.path.splitext(l)
        dataframe = loader(path_dir, file_name, file_extension)
        dumper(dataframe, folder_temporary, file_name, 'PICKLE')

    return len(list_files) - 1

def close_dir(path_dir):
    try:
        shutil.rmtree(path_dir, ignore_errors=False)
    except:
        pass