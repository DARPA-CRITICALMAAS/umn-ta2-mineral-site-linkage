import os
import pickle
import geopandas as gpd

from json import loads, dumps

from minelink.params import *
from minelink.m0_load_input.save_ckpt import open_ckpt_dir

def save_output_json(df_data, list_path, file_name):
    path_dir = ''
    for i in list_path:
        path_dir = os.path.join(path_dir, i)

    json_df = df_data.to_json(orient='index', default_handler=str)

    json_data = loads(json_df)
    obj_data = dumps(json_data, indent=4)

    open_ckpt_dir(list_path=[PATH_OUTPUT_DIR])

    with open(os.path.join(path_dir, file_name+'.json'), 'w') as handle:
        handle.write(obj_data)

def save_as_geojson(data, path_dir, file_name):        
    data = data[['source_name', 'site_name', 'geometry', 'GroupID']]

    data.to_file(os.path.join(path_dir, file_name+'.geojson'), driver='GeoJSON')