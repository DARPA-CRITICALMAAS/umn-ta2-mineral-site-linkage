import regex as re

import warnings
import numpy as np
import pandas as pd
import time

from minelink.params import *
from minelink.m0_save_and_load.load_data import *
from minelink.m0_save_and_load.save_ckpt_as_pickle import save_ckpt
from minelink.m1_preprocessing.datadictionary_processing import *

warnings.simplefilter(action='ignore', category=FutureWarning)

def get_unique_id_column(df, dict_dictionary):
    """
    : input: df (pandas dataframe) = 
    : input: df_dictionary (pandas dataframe) = 
    """
    len_df = df.shape[0]

    # Get count of unique items in each dataframe
    try: 
        df_tmp = df.drop(['geometry'], axis=1)
    except:
        df_tmp = df
    count_unique_items = df_tmp.apply(pd.Series.nunique)
    count_unique_items = count_unique_items[count_unique_items == len_df]

    if len(count_unique_items) == 0:
        return False

    for i in count_unique_items.index.tolist():
        splitted_col_name = re.split('[^A-Za-z]', i.lower())
        unique_label = ''.join(re.findall('[A-Za-z]', i))

        if 'id' in splitted_col_name:
            return i
        # elif re.search('([^A-Za-z]+Id|Id[^A-Za-z]+)', i):
        #     print(i)

        if unique_label not in dict_dictionary.keys():
            continue

        elif re.search('unique', dict_dictionary[unique_label].lower()):
            return i
        
        else:
            return False

def get_site_name_columns(df_data, df_dictionary, col_available):
    df_remaining = df_data[list(col_available)]
    df_remaining = df_remaining.select_dtypes(exclude=['number', 'bool'])
    col_remaining = list(df_remaining.columns)

    col_names = find_from_dictionary(df_dictionary, col_remaining, ['name'])

    # TODO: concat columns with the col_names

    return col_names

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
    col_remaining = list(col_to_compare)

    list_col_return = find_from_dictionary(df_dictionary, col_remaining, ['latitude', 'longitude', 'crs'])

    df_latitude = df_dictionary[df_dictionary['label'] == col_latitude[0]]
    description = df_latitude['long'].values[0]
    crs_val = get_crs_value(description)

    return col_longitude, col_latitude, crs_val

def find_columns(df, source_alias_code, source_name):
    df_dictionary = load_file(path_dir=PATH_TMP_DIR, additional=source_alias_code, file_name='dictionary', extension='.pkl')

    df_current_dictionary = df_dictionary
    df_current_dictionary.insert(loc=0, column='source', value=source_name)

    # TODO: bring it back later
    # try: 
    #     df_previous_archive = load_file(PATH_SRC_DIR, 'dictionary_archive')
    #     df_dictionary_archive = pd.concat([df_previous_archive, df_current_dictionary], axis=0)
    # except:
    #     df_dictionary_archive = df_current_dictionary
    # save_ckpt(df_dictionary_archive, PATH_SRC_DIR, 'dictionary_archive')

    start_time = time.time()

    col_available = set(list(df.columns))

    df_dictionary['reduced_label'] = df_dictionary['label'].apply(lambda x: ''.join(re.findall('[A-Za-z]', x)))
    dict_dictionary = dict(zip(df_dictionary['reduced_label'], df_dictionary['long']))

    df_dictionary = df_dictionary.drop(['reduced_label'], axis=1)

    col_unique_id = get_unique_id_column(df, dict_dictionary)
    col_available = col_available - set([col_unique_id]) if col_unique_id else col_available

    dict_loc_col_map, col_textual_location = get_textual_location_columns(col_available)
    col_available = col_available - set(col_textual_location)

    col_sitename = get_site_name_columns(df, df_dictionary, col_available)
    col_geocoordinates = get_geocoordinate_columns(df, df_dictionary, col_available)

    run_time = time.time() - start_time

    return col_unique_id, dict_loc_col_map, col_geocoordinates, col_sitename