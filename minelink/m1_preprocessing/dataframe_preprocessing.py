import os
import time
import logging

import pandas as pd
import geopandas as gpd
import pickle5 as pickle
import regex as re
from tqdm import tqdm

from minelink.params import *
from minelink.m0_save_and_load.save_ckpt_as_pickle import *
from minelink.m1_preprocessing.convert_to_geodataframe import *
from minelink.m1_preprocessing.column_mapping import find_columns
from minelink.m1_preprocessing.datadictionary_processing import *

def convert_df_to_dict(df_data):
    dict_data = df_data.to_dict('index')

    return dict_data

def create_dict_info(df_data, col_name):
    df_info = df_data[col_name]
    df_info.columns = ['0']
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

def create_dict_sameas(df_data, source_name):
    df_sameas = df_data
    df_sameas['record_id'] = df_sameas.index.astype(str)
    df_sameas['record_id'] = df_sameas['record_id'].apply(lambda x: re.split('_', x)[1])

    df_sameas.insert(loc=0, column='source', value=source_name)

    df_sameas = df_sameas[['source', 'record_id']]

    dict_sameas = convert_df_to_dict(df_sameas)

    return dict_sameas

def separate_dataframe(df, path_to_store, source_alias_code, source_name):
    # logging.basicConfig(filename='preprocessing.log', level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    # logging.warning('is when preprocessing module started.')

    save_ckpt(df, path_to_store, 'raw')

    col_unique_id, dict_loc_col_map, col_geocoordinates, col_sitename = find_columns(df, source_alias_code, source_name)

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

        else:
            precise_col_longitude = col_longitude[0]
            precise_col_latitude = col_latitude[0]

            df_len = pd.DataFrame()
            for i in col_longitude:
                df_len[i] = df[i].apply(lambda x: len(re.split('\.', x)[1]))

            df_len = df_len.max()

            for i in col_latitude:
                print()

            df = convert_to_gdb(df, precise_col_longitude, precise_col_latitude, crs=crs_val)

        # TODO: determine which one to select if there are multiple longitude/latitude values
        # float with more decimal places?
        # split at dot, take the second part, get the length, select one with the longest length in the column, use that column as the the latitude value to convert to gdb

    df = df.rename(columns=dict_loc_col_map)

    # print(col_longitude, col_latitude, col_sitename)
    
    dict_info = create_dict_info(df, col_sitename)
    dict_sameas = create_dict_sameas(df, source_name)
    dict_loc, dict_geo = create_dict_location(df, source_name)

    save_ckpt(dict_info, path_to_store, 'site_name')
    save_ckpt(dict_sameas, path_to_store, 'same_as')
    save_ckpt(dict_loc, path_to_store, 'location_info')
    save_ckpt(dict_geo, path_to_store, 'geometry')

    # TODO: create dataframe consisting of relevant information (that will be used for linking)
    # drop all column relevant to latitude, longitude, crs, (maybe textual location and unique id)
    # df_link = df.drop([col_unique_id], axis=1)
    df_link = df

    save_ckpt(df_link, path_to_store, 'df_tolink')

    return df_link