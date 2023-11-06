import os

import pickle5 as pickle
from json import loads, dumps

import pandas as pd
import geopandas as gpd
from shapely import wkt
from transformers import AutoTokenizer, AutoModel

from utils.params import *

def gdb_load(directory, filename):
    """
    gdb_load function

    : input: directory = 
    : input: filename = 

    : return: gpd_file = 
    """
    gpd_file = gpd.read_file(os.path.join(directory, filename+'.gdb'), driver="OpenFileGDB")

    return gpd_file

def csv_load(directory, filename):
    """
    csv_load function

    : input: directory = 
    : input: filename = 

    : return: csv_file = 
    """
    csv_file = pd.read_csv(os.path.join(directory, filename + '.csv'))

    return csv_file

def pickle_dump(data, directory, filename):
    """
    picle_dump function
    
    : input: data = 
    : input: directory = 
    : input: filename = 
    
    : return: None
    : file IO: 
    """
    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(os.path.join(directory, filename+'.pkl'), 'wb') as handle:
        pickle.dump(data, handle, protocol=pickle.HIGHEST_PROTOCOL)

def pickle_load(directory, filename):
    """
    pickle_load function

    : input: directory = 
    : input: filename = 

    : return: dataframe = 
    """
    with open(os.path.join(directory, filename+'.pkl'), 'rb') as handle:
        dataframe = pickle.load(handle)

    return dataframe

def json_dump(dataframe, directory, filename):
    """
    json_dump function

    : input: dataframe = 
    : input: directory = 
    : input: filename = 

    : return: None
    : file IO: 
    """
    if not os.path.exists(directory):
        os.makedirs(directory)

    json_df = dataframe.to_json(orient='index', default_handler=str)    # default handler set to prevent iteration overflow

    json_data = loads(json_df)
    obj_data = dumps(json_data, indent=4)

    with open(os.path.join(directory, filename+'.json'), 'w') as handle:
        handle.write(obj_data)

def json_load():
    return 0

def model_load(model_name, mode_hidden=True):
    """
    transformer_load function

    : input: modelname = 

    : return: tokenizer = 
    : return: model = 
    """
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModel.from_pretrained(model_name, output_hidden_states=mode_hidden)

    return tokenizer, model

def directory_load(directory):
    """
    directory_load function

    : input: directory

    : return: 
    """
    list_dir_items = os.listdir(directory)
    list_files = [f for f in list_dir_items if os.path.isfile(directory+'/'+f)]

    return 0

def geojson_dump(dataframe, directory, filename):
    """
    geojson_dump function
    
    : input: dataframe = input dataframe that would be outputted out as a geojson file
    : input: directory = the directory where the geojson file should be saved to
    : input: filename = the name of the geojson file
    
    : return: None
    : file IO: geojson file consisting of the site name, geometry, and groupoID
    """

    if not os.path.exists(directory):
        os.makedirs(directory)

    dataframe = dataframe[['site_name', 'geometry', 'GroupID']]
    dataframe['site_name'] = dataframe['site_name'].apply(lambda x: x[0])

    dataframe = dataframe.apply(pd.to_numeric, errors='ignore')
    gdf = gpd.GeoDataFrame(
        dataframe
    )

    gdf = gdf.set_geometry('geometry')
    gdf.crs = 'NAD83'

    gdf.to_file(os.path.join(directory, filename+'.geojson'), driver='GeoJSON')

def raw_geojson_dump(dataframe, directory, filename):
    """
    raw_geojson_dump function saves the raw dataframe (in other words, not yet grouped) into a geojson format
    
    : input: dataframe = input dataframe that would be outputted out as a geojson file
    : input: directory = the directory where the geojson file should be saved to
    : input: filename = the name of the geojson file
    
    : return: None
    : file IO: geojson file consisting of the site name, geometry, and groupoID
    """
    if not os.path.exists(directory):
        os.makedirs(directory)

    dataframe = dataframe[['depname', 'geometry']]
    dataframe['depname'] = dataframe['depname'].apply(lambda x: x[0])

    dataframe = dataframe.apply(pd.to_numeric, errors='ignore')
    gdf = gpd.GeoDataFrame(
        dataframe
    )

    gdf = gdf.set_geometry('geometry')
    gdf.crs = 'NAD83'

    gdf.to_file(os.path.join(directory, filename+'.geojson'), driver='GeoJSON')