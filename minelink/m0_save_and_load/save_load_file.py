#!/usr/bin/python

import os
import shutil

import regex as re
import pandas as pd
import geopandas as gpd
from shapely import wkt
import pickle5 as pickle

from json import loads, dumps

def load_file(path_dir, file_name, extension):
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
    
    elif extension == '.xls' or extension == '.xlsx':
        return(pd.read_excel(os.path.join(path_dir, file_name + extension)))
    
    # TODO: Somethng is wrong here; it needs to be editted so that later user inputs can also be read
    # elif extension == '.json':
    #     f = open(os.path.join(path_dir, file_name + extension), 'r')
    #     data  = loads(f)
    #     return data

def dump_file(data, path_dir, file_name, save_format):
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

def move_file(path_org_file, path_mv_dir):
    try: 
        shutil.copy(path_org_file, path_mv_dir)
        return 0
    except:
        return -1