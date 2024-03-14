import os
import pickle
import geopandas as gpd

from json import dump

from fusemine.params import *
from fusemine.m0_load_input.save_ckpt import open_ckpt_dir

# def save_output_json(pl_data, list_path, file_name):
#     path_dir = ''
#     for i in list_path:
#         path_dir = os.path.join(path_dir, i)

#     pl_data.write_ndjson(os.path.join(path_dir, file_name+'.json'))
#     return 0

def save_output_json(df_data, file_name, list_path=[PATH_OUTPUT_DIR]):
    path_dir = ''
    for i in list_path:
        path_dir = os.path.join(path_dir, i)

    open_ckpt_dir(list_path=[PATH_OUTPUT_DIR])

    df_data_asdict = df_data.to_dict(orient='records')
    df_with_key = {"MineralSite":df_data_asdict}

    with open(os.path.join(path_dir, file_name+'.json'), 'w') as f:
        dump(df_with_key, f, indent=4, default=str)

    # json_df = df_data.to_json(os.path.join(path_dir, file_name+'.json'), 
    #                           orient='records', 
    #                           lines=True, 
    #                           default_handler=str)
        
def save_output_geojson(gdf_data, file_name, list_path=[PATH_OUTPUT_DIR]):
    path_dir = ''
    for i in list_path:
        path_dir = os.path.join(path_dir, i)

    open_ckpt_dir(list_path=[PATH_OUTPUT_DIR])

    # added to ensure that there are no records recorded twice
    

    gdf_data.to_file(os.path.join(path_dir, file_name+'.geojson'), driver='GeoJSON')

    # df_data_asdict = gdf_data.to_dict(orient='records')
    # df_with_key = {"MineralSite":df_data_asdict}

    # with open(os.path.join(path_dir, file_name+'.json'), 'w') as f:
    #     dump(df_with_key, f, indent=4, default=str)

    # json_df = df_data.to_json(os.path.join(path_dir, file_name+'.json'), 
    #                           orient='records', 
    #                           lines=True, 
    #                           default_handler=str)

def save_as_geojson(data, path_dir, file_name):        
    data = data[['source_name', 'site_name', 'geometry', 'GroupID']]

    data.to_file(os.path.join(path_dir, file_name+'.geojson'), driver='GeoJSON')