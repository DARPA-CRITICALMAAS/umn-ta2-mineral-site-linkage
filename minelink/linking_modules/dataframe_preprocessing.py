import os
import pandas as pd
import geopandas as gpd
import pickle5 as pickle
import regex as re
from tqdm import tqdm

from minelink.linking_modules.column_mapping import *
from minelink.loadsave_modules.directory_related import *
# from column_mapping import *

def create_dict_info(df_data, col_name):
    # Creates a dictionary with 'id' as the key and 'site_name' as the value
    dict_info = df_data[col_name].to_dict('index')
    
    return dict_info

def create_dict_location(df_data, crs, location):
    # Parts required for location
    col_location = set(['country', 'state'])
    column_location = set(['idx', 'geometry', 'country', 'state', 'crs', 'location_source', 'index'])
    # location point, country (optional), state(optional), location_source_record_id, location_source, crs
    dict_location = df_data
    return dict_location

def create_dict_sameas(df_data):
    """
    input: dataframe with the original data
    return: dictionary with 'id' as key and original data as values
    """
    try:
        df_data = df_data.drop(['geometry'], axis=1)
    except:
        pass

    # dict_sameas = df_data.set_index('id')
    dict_sameas = df_data.to_dict('index')

    return dict_sameas

def tmp_separate_dataframe(df_data, df_dict, source_name):
    # Appends a temporary index (that will be used for this program)
    df_data['id'] = source_name + df_data.index.astype('string')

    # Find columns related to site name (as col_name) and latitude, longitude, crs (as list_col_remove)
    col_name, list_col_geometry, bool_geometry = find_name_geom_columns(df_data, df_dict)

    if not bool_geometry:
        df_data = gpd.GeoDataFrame(
            df_data, geometry=gpd.points_from_xy(list_col_geometry[0], list_col_geometry[1]), crs=list_col_geometry[2]
        )

        print('no')

    dict_info = create_dict_info(df_data, col_name)

    # Creates a 'sameas' dictionary
    dict_sameas = create_dict_sameas(df_data)

    # create_dict_location(df_data, crs, location)
    
    df_data = df_data.drop(list_col_geometry, axis=1)

    # Checks whether the dataframe is an acceptable geodataframe (a.k.a. can automatically extract latitude longitude crs)
    # try:
    #     crs = df_data.crs
    #     location = df_data.geometry
    #     bool_loc_available = True
    # except:
    #     crs = 'None'
    #     location = 'None'
    #     bool_loc_available = False

    # dict_location = create_dict_location(df_data)

    return df_data, dict_sameas

def separate_dataframe(df_data, source_name):
    # Appends a temporary index (that will be used for this program)
    df_data['id'] = source_name + df_data.index.astype('string')
    df_data = df_data.set_index('id')

    # dict_info = create_dict_info(df_data, 'site_name')
    dict_sameas = create_dict_sameas(df_data)

    return df_data, dict_sameas

def preprocessing(bool_full, bool_location):
    list_items = os.listdir('./temporary')

    for i in list_items:
        file_name, file_extension = os.path.splitext(i)
        source_name = re.split('[^A-Za-z]', file_name)[0]
        print("  ", file_name, "as", source_name)
        # TODO: add portion to check source name? maybe>

        check_dir('./'+source_name+'_'+file_name)

        dataframe = loader('./temporary', file_name, file_extension)

        df_data, dict_sameas = separate_dataframe(dataframe, source_name)

        print(df_data, dict_sameas)
        # dumper(df_data, './'+source_name+'_'+file_name, 'data', 'PICKLE')
        # dumper(dict_sameas, './'+source_name+'_'+file_name, 'same_as', 'PICKLE')

        # print(dataframe)

    # if bool_location:
    #     # separate_dataframe()
    #     print(list_items, "Location")
    #     return 1
    
    # print(list_items, "Full")
    # return 2