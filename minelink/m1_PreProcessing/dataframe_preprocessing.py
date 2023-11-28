import os
import pandas as pd
import geopandas as gpd
import pickle5 as pickle
import regex as re
from tqdm import tqdm

from minelink.params import *
from minelink.m1_PreProcessing.convert_to_geodataframe import *
from minelink.m1_PreProcessing.column_mapping import find_columns
from minelink.m1_PreProcessing.datadictionary_processing import *

def convert_df_to_dict(df_data):
    dict_data = df_data.to_dict('index')

    return dict_data

def create_dict_info(df_data, col_name):
    df_info = df_data[col_name]
    dict_info = convert_df_to_dict(df_info)

    return dict_info

def create_dict_geometry(df_loc):
    df_geo = df_loc[['geometry']]

    dict_geo = convert_df_to_dict(df_geo)

    return dict_geo

def create_dict_location(df_data, source_name):
    df_loc = df_data
    df_loc.insert(loc=0, column='location_source', value=source_name)
    df_loc.insert(loc=0, column='crs', value='WGS84')

    col_location = set(['geometry', 'country', 'state_or_province', 'crs', 'location_source', 'location_source_record_id'])
    col_available = set(list(df_data.columns))
    col_in_common = list(col_location & col_available)

    df_loc = df_loc[col_in_common]

    dict_geo = create_dict_geometry(df_loc)
    dict_loc = convert_df_to_dict(df_loc)

    return dict_loc, dict_geo

def separate_dataframe(df, source_alias_code, source_name):
    # TODO: call column mapping to find site name, latitude, longitude, crs, (maybe substring match for state province), unique_id
    # input: dataframe and source_alias_code
    # [col_site_name, col_other_names], [col_latitude], [col_longitude], [col_crs], col_state_province, [unique_id]

    col_to_drop = []

    col_unique_id, dict_loc_col_map, col_geocoordinates, col_available = find_columns(df, source_alias_code)

    if col_unique_id:
        df['idx'] = source_alias_code + '_' + df[col_unique_id].astype(str)
        df['location_source_record_id'] = df[col_unique_id].astype(str)
    else:
        df['idx'] = source_alias_code + '_' + df.index.astype(str)
        df['location_source_record_id'] = df.index.astype(str)

    df = df.set_index('idx')

    try:
        df = unify_gdb_crs(df)
    except:
        col_longitude = col_geocoordinates[0]
        col_latitude = col_geocoordinates[1]
        crs_val = col_geocoordinates[2]

        if len(col_longitude)==1 and len(col_latitude)==1:
            df = convert_to_gdb(df, col_longitude[0], col_latitude[0], crs=crs_val)

        # TODO: determine which one to select if there are multiple longitude/latitude values
        # float with more decimal places?

    df = df.rename(columns=dict_loc_col_map)

    dict_sameas = convert_df_to_dict(df)
    dict_loc, dict_geo = create_dict_location(df, source_name)

    # TODO: create dataframe consisting of relevant information (that will be used for linking)
    # drop all column relevant to latitude, longitude, crs, (maybe textual location and unique id)
    
    # df_link = df.drop([col_unique_id], axis=1)
    df_link = df

    # print(df_link.columns)

    return dict_sameas, dict_loc, dict_geo, df_link