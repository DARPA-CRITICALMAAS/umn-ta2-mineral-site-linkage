import os
import pandas as pd
import geopandas as gpd
from tqdm import tqdm

from minelink.linking_modules.column_mapping import *
# from column_mapping import *

def create_dict_info(df_data, col_name):
    # Creates a dictionary with 'id' as the key and 'site_name' as the value
    dict_df = df_data.set_index('id')
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

    dict_sameas = df_data.set_index('id')
    dict_sameas = df_data.to_dict('index')

    return dict_sameas

def separate_dataframe(df_data, df_dict, source_name):
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



def create_output_dataframe(df_linked, dict_info, dict_location, dict_sameas):
    df_output = df_linked

    # based on idx in df_linked call dict info, dict location, an dict sameas
    # for dict location, create a dataframe and select centroid

    return df_linked