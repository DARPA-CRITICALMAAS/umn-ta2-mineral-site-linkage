import regex as re

import numpy as np
import pandas as pd

from minelink.params import *
from minelink.m0_save_and_load.save_load_file import *
from minelink.m1_preprocessing.datadictionary_processing import *

def get_unique_id_column(df):
    len_df = df.shape[0]

    # Get count of unique items in each dataframe
    df_tmp = df.drop(['geometry'], axis=1)
    count_unique_items = df_tmp.apply(pd.Series.nunique)
    count_unique_items = count_unique_items[count_unique_items == len_df]

    if len(count_unique_items) == 0:
        return False

    for i in count_unique_items.index.tolist():
        splitted_col_name = re.split('[^A-Za-z]', i.lower())

        if 'id' in splitted_col_name:
            return i
        # elif re.search('([^A-Za-z]+Id|Id[^A-Za-z]+)', i):
        #     print(i)
        else:
            return False

def get_site_name_columns(df_data, df_dictionary, col_available):
    df_remaining = df_data[list(col_available)]
    df_remaining = df_remaining.select_dtypes(exclude=['number', 'bool'])
    col_remaining = list(df_remaining.columns)

    df_remaining.to_csv('tmp.csv')

    print(df_remaining.columns)

    # TODO: find sitename and other names column (must have at least one column)

    col_names = []
    col_names = find_from_dictionary(df_dictionary, col_remaining, ['site_name', 'other_name'])

def get_textual_location_columns(col_available):
    dict_loc_col_map = {}
    col_textual_location = []

    for i in col_available:
        if i.lower()=='country':
            dict_loc_col_map[i] = 'country'
            col_textual_location.append(i)
        elif re.search('state', i.lower()) or re.search('province', i.lower()):
            dict_loc_col_map[i] = 'state_or_province'
            col_textual_location.append(i)

    return dict_loc_col_map, col_textual_location

def get_geocoordinate_columns(df_data, df_dictionary, col_available):
    # TODO: find latitude, longitude, crs columns
    col_latitude = []
    col_longitude = []
    col_crs = []
    
    df_data = df_data.select_dtypes(include=['number'])
    col_to_compare = list(set(list(df_data.columns)) & col_available)

    for i in col_to_compare:
        if re.search('latitude', i.lower()):
            col_latitude.append(i)
        elif re.search('longitude', i.lower()):
            col_longitude.append(i)
        elif re.match('crs', i.lower()):
            col_crs.append(i)

    col_to_compare = set(col_to_compare) - set(col_latitude) - set(col_longitude) - set(col_crs)

    # TODO: Call finding in dictionary function

    list_col_return, crs_val = find_from_dictionary(df_dictionary, col_to_compare, ['latitude', 'longitude', 'crs'])

    # TODO: [longitudes], [latitudes], crs_val

    return col_longitude, col_latitude, crs_val

def find_columns(df, source_alias_code):
    df_dictionary = load_file(path_dir=PATH_TMP_DIR, additional=source_alias_code, file_name='dictionary', extension='.pkl')
    dict_alias = load_file(path_dir=PATH_TMP_DIR, file_name='code_alias', extension='.pkl')
    source_name = dict_alias[source_alias_code]

    df_dictionary_archive = df_dictionary
    df_dictionary_archive.insert(loc=0, column='source', value=source_name)
    dump_file(df_dictionary_archive, PATH_SRC_DIR, 'dictionary_archive', 'PICKLE')

    # df_target = load_file(PATH_SRC_DIR, 'dictionary_target', '.pkl')

    col_available = set(list(df.columns))

    # [col_site_name, col_other_names]

    col_unique_id = get_unique_id_column(df)
    col_available = col_available - set([col_unique_id]) if col_unique_id else col_available

    dict_loc_col_map, col_textual_location = get_textual_location_columns(col_available)
    col_available = col_available - set(col_textual_location)

    get_site_name_columns(df, df_dictionary, col_available)

    col_geocoordinates = get_geocoordinate_columns(df, df, col_available)

    return col_unique_id, dict_loc_col_map, col_geocoordinates, col_available