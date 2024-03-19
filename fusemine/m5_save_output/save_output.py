import os
import pickle
import polars as pl
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

# def save_output_json(df_data, file_name, list_path=[PATH_OUTPUT_DIR]):
#     path_dir = ''
#     for i in list_path:
#         path_dir = os.path.join(path_dir, i)

#     open_ckpt_dir(list_path=[PATH_OUTPUT_DIR])

#     df_data_asdict = df_data.to_dict(orient='records')
#     df_with_key = {"MineralSite":df_data_asdict}

#     with open(os.path.join(path_dir, file_name+'.json'), 'w') as f:
#         dump(df_with_key, f, indent=4, default=str)

    # json_df = df_data.to_json(os.path.join(path_dir, file_name+'.json'), 
    #                           orient='records', 
    #                           lines=True, 
    #                           default_handler=str)

def save_output_csv(df_data, file_name, list_path=[PATH_OUTPUT_DIR]):
    path_dir = ''
    for i in list_path:
        path_dir = os.path.join(path_dir, i)

    open_ckpt_dir(list_path=[PATH_OUTPUT_DIR])

    df_sameas = df_data[['same_as']]

    pl_sameas = pl.from_pandas(df_sameas)
    pl_sameas = pl_sameas.filter(
        pl.col('same_as').list.len() > 1
    ).with_columns(
        tmp1 = pl.col('same_as').list.get(0)
    )

    pl_sameas = pl_sameas.unnest('tmp1').with_columns(
        source1 = pl.col('source_id').cast(pl.Utf8) + '_' + pl.col('record_id').cast(pl.Utf8)
    ).drop(['source_id', 'record_id'])

    pl_sameas = pl_sameas.explode('same_as')

    pl_sameas = pl_sameas.unnest('same_as').with_columns(
        source2 = pl.col('source_id').cast(pl.Utf8) + '_' + pl.col('record_id').cast(pl.Utf8)
    ).drop(['source_id', 'record_id']).filter(
        pl.col('source1') != pl.col('source2')
    )

    pl_sameas.write_csv(os.path.join(path_dir, file_name+'.csv'), separator=",")


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