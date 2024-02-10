import polars as pl
import pandas as pd
import geopandas as gpd
import pickle5 as pickle

import regex as re
import shapely.wkt

from minelink.params import *
from minelink.m0_load_input.save_ckpt import *
from minelink.m0_load_input.load_data import load_file

def convert_df_to_dict(df_data):
    df_data = df_data.set_index('idx')
    dict_data = df_data.to_dict('index')

    tmp_data = pd.DataFrame(dict_data)
    dict_data = {col: tmp_data[col].dropna().to_dict() for col in tmp_data}

    return dict_data

def degree_to_decimal(s):
    s = s.strip(' \(\)')

    try:
        degrees, minutes, seconds, direction = re.split('[°\'"]+', s)

        dd = float(degrees) + float(minutes)/60 + float(seconds)/(60*60)
        if direction in ['S', 'W']:
            dd *= -1

        return dd
    
    except:
        try:
            degrees, direction = re.split('[°]+', s)

            dd = float(degrees)
            if direction in ['S', 'W']:
                dd *= -1

            return dd
        
        except:
            return s

def tmp(str_location):
    try:
        geometry = shapely.wkt.loads(str_location)
    except:
        geometry = str_location.replace(', ', ' ')
        geometry = shapely.wkt.loads(geometry)

    if re.match('MULTIPOINT', str_location):
        return [p for p in geometry.geoms]

    return [geometry]

def separate_long_lat(wkt_location):
    try:
        longitude = wkt_location.x
        latitude = wkt_location.y

        return [[longitude], [latitude]]

    except:
        return [[p.x for p in wkt_location.geoms], [p.y for p in wkt_location.geoms]]

def gpd_geometry(str_location, crs):
    try:
        geometry = shapely.wkt.loads(str_location)
    except:
        geometry = str_location.replace(', ', ' ')
        geometry = shapely.wkt.loads(geometry)

    gpd_geom = gpd.GeoDataFrame({'geometry': [geometry]}, crs=crs)

    return gpd_geom.at[0, 'geometry']

def create_location_info(pl_location):
    df_location = pl_location.to_pandas()
    df_location['location'] = df_location.apply(lambda x: gpd_geometry(x['location'], x['crs']), axis=1)

    dict_location_info = convert_df_to_dict(df_location)
    
    df_location['location'] = df_location['location'].apply(lambda x: separate_long_lat(x))
    df_longlat = df_location['location'].apply(pd.Series).rename(
        {0: 'longitude',
         1: 'latitude'},
         axis=1
    )
    df_location = df_location[['idx']]
    df_location = pd.concat(
        [df_location, df_longlat],
        axis=1
    )

    pl_geom = pl.from_pandas(
        df_location
    ).explode(
        'longitude', 'latitude'
    )

    return pl_geom, dict_location_info

def process_schema(mss_code):
    pl_data = load_file(list_path=[PATH_TMP_DIR, mss_code], 
                        file_name='raw', 
                        file_extension='.pkl')
    
    df_info  = pl_data.select(
        pl.col(['idx', 'source_id', 'record_id', 'name'])
    )

    df_sameas = df_info.select(
        pl.col(['idx', 'source_id', 'record_id'])
    ).to_pandas()

    dict_basic_info = convert_df_to_dict(df_info.to_pandas())

    save_ckpt(data=dict_basic_info, 
              list_path=[PATH_TMP_DIR, 'mss'],
              file_name='basic_info')
    
    dict_sameas = convert_df_to_dict(df_sameas)
    save_ckpt(data=dict_sameas, 
              list_path=[PATH_TMP_DIR, 'mss'],
              file_name='same_as')
    
    df_location  = pl_data.select(
        pl.col(['idx', 'location', 'crs', 'country', 'state_or_province'])
    )
    df_geometry, dict_location_info = create_location_info(df_location)

    save_ckpt(data=df_geometry, 
              list_path=[PATH_TMP_DIR, 'mss'],
              file_name='df_geometry')
    
    save_ckpt(data=dict_location_info, 
              list_path=[PATH_TMP_DIR, 'mss'],
              file_name='location_info')