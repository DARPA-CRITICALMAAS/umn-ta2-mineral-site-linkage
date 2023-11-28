import os
import pandas as pd
import geopandas as gpd
import pickle5 as pickle
import regex as re
from tqdm import tqdm

from minelink.params import *
# from minelink.m1_PreProcessing.column_mapping import *
from minelink.m1_PreProcessing.convert_to_geodataframe import *
# from minelink.m0_SaveAndLoad.directory_related import *
# from minelink.m1_PreProcessing.column_mapping import *

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

    col_location = set(['geometry', 'country', 'state', 'crs', 'location_source', 'id'])
    col_available = set(list(df_data.columns))
    col_in_common = list(col_location & col_available)

    df_loc = df_loc[col_in_common]
    dict_geo = create_dict_geometry(df_loc)

    dict_loc = convert_df_to_dict(df_loc)

    return dict_loc, dict_geo

# def create_dict_location(df_data, crs, location):
#     # Parts required for location
#     col_location = set(['country', 'state'])
#     column_location = set(['idx', 'geometry', 'country', 'state', 'crs', 'location_source', 'index'])
#     # location point, country (optional), state(optional), location_source_record_id, location_source, crs
#     dict_location = df_data
#     return dict_location

# def create_dict_sameas(df_data):
#     """
#     input: dataframe with the original data
#     return: dictionary with 'id' as key and original data as values
#     """
#     try:
#         df_data = df_data.drop(['geometry'], axis=1)
#     except:
#         pass

#     # dict_sameas = df_data.set_index('id')
#     dict_sameas = df_data.to_dict('index')

#     return dict_sameas

# def tmp_separate_dataframe(df_data, df_dict, source_name):
#     # Appends a temporary index (that will be used for this program)
#     df_data['id'] = source_name + df_data.index.astype('string')

#     # Find columns related to site name (as col_name) and latitude, longitude, crs (as list_col_remove)
#     col_name, list_col_geometry, bool_geometry = find_name_geom_columns(df_data, df_dict)

#     if not bool_geometry:
#         df_data = gpd.GeoDataFrame(
#             df_data, geometry=gpd.points_from_xy(list_col_geometry[0], list_col_geometry[1]), crs=list_col_geometry[2]
#         )

#         print('no')

#     dict_info = create_dict_info(df_data, col_name)

#     # Creates a 'sameas' dictionary
#     dict_sameas = create_dict_sameas(df_data)

#     # create_dict_location(df_data, crs, location)
    
#     df_data = df_data.drop(list_col_geometry, axis=1)

#     return df_data, dict_sameas

def separate_dataframe(df, source_alias_code, source_name):
    df['idx'] = source_alias_code + '_' + df.index.astype('string')
    df = df.set_index('idx')

    # TODO: call column mapping to find site name, latitude, longitude, crs, (maybe substring match for state province), unique_id
    # input: dataframe and source_alias_code
    # [col_site_name, col_other_names], [col_latitude], [col_longitude], [col_crs], col_state_province, [unique_id]
    # if df is not a geodataframe call convert to geodataframe with lat, long, {crs; default=WGS84}

    dict_sameas = convert_df_to_dict(df)
    dict_loc, dict_geo = create_dict_location(df, source_name)

    return dict_sameas, dict_loc, dict_geo

# def separate_dataframe(df_data, source_name):
#     # Appends a temporary index (that will be used for this program)
#     df_data['id'] = source_name + df_data.index.astype('string')
#     df_data = df_data.set_index('id')

#     # dict_info = create_dict_info(df_data, 'site_name')
#     dict_sameas = create_dict_sameas(df_data)

#     return df_data, dict_sameas

# def preprocessing(bool_full, bool_location):
#     list_items = os.listdir(PATH_TMP_DIR)
#     dict_tmp_alias = {}
#     over_char = 'a'
#     alias_char = 'a'

#     for i in list_items:
#         file_name, file_extension = os.path.splitext(i)
#         try:
#             unique_id = re.sub('_', '/', file_name)
#         except:
#             unique_id = file_name

#         dict_tmp_alias[over_char+alias_char] = unique_id

#         dataframe = loader(PATH_TMP_DIR, file_name, file_extension)

#         PATH_TMP_DATA_DIR = os.path.join(PATH_TMP_DIR, file_name)
#         check_dir(PATH_TMP_DATA_DIR)

#         # print(file_name)
#         df_data, dict_sameas = separate_dataframe(dataframe, unique_id)
#         # dictionary_unavailable(df_data)

#         alias_char = chr(ord(alias_char) + 1) 
#         over_char = chr(ord(over_char) + 1) if alias_char == 'a' else over_char
#         # dumper(df_data, PATH_TMP_DATA_DIR, 'data', 'PICKLE')
#         # dumper(dict_sameas, PATH_TMP_DATA_DIR, 'dict_sameas', 'PICKLE')

#     print(dict_tmp_alias)
#     # if bool_location:
#     #     # separate_dataframe()
#     #     print(list_items, "Location")
#     #     return 1
    
#     # print(list_items, "Full")
#     # return 2