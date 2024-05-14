import os
import logging

import pickle
import polars as pl
import geopandas as gpd
from shapely import wkt
from json import loads, dump

from utils.unify_coordinate_system import *
from utils.convert_dataframe import *

def clean_nones(input_object: dict | list) -> dict | list:
    """
    Recursively remove all None values from either a dictionary or a list, and returns a new dictionary or list without the None values

    : param: input_object = either a dictionary or a list type that may or may not consist of a None value
    : return:= either a dictionary or a list type (same as the input) that does not consists of any None values
    """

    # List case
    if isinstance(input_object, list):
        return [clean_nones(x) for x in input_object if x is not None and x != ""]
    
    # Dictionary case
    elif isinstance(input_object, dict):
        return {
            key: clean_nones(value)
            for key, value in input_object.items()
            if value is not None and value != ""
        }

    else:
        return input_object

def as_pkl(input_data, output_file_location:str) -> int:
    with open(output_file_location, 'wb') as handle:
        pickle.dump(handle, input_data, protocol=pickle.HIGHEST_PROTOCOL)

def as_csv(pl_data, output_directory: str, output_file_name: str, bool_sameas: bool):
    if bool_sameas:
        # Filtering out with GroupID -1 (i.e, no group info) and those that are length 1
        logging.info(f'Saving same_as data to {os.path.join(output_directory, output_file_name)}')

        pl_data = pl_data.select(
            pl.col(['ms_uri', 'GroupID'])
        ).with_columns(
            pl.col('ms_uri').str.split(',')
        ).explode(
            'ms_uri'
        )
        logging.info(f'\t{pl_data.shape[0]} mineral records')
        
        pl_data = pl_data.group_by(
            'GroupID'
        ).agg(
            [pl.all()]
        ).with_columns(
            linked_count = pl.col('ms_uri').list.len()
        )
        logging.info(f'\t{pl_data.shape[0]} grouped sites')
        
        # Converting data to two column csv
        pl_data = pl_data.select(
            ms_uri_1 = pl.col('ms_uri')
        ).with_columns(
            ms_uri_2 = pl.col('ms_uri_1').list.get(0)
        ).explode(
            'ms_uri_1'
        )

    # Saving to {output_directory}/{output_file_name}
    _, file_extension = os.path.splitext(output_file_name)

    if file_extension:
        output_file_location = os.path.join(output_directory, output_file_name)
    else:
        output_file_location = os.path.join(output_directory, output_file_name+'.csv')

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)          
    pl_data.write_csv(output_file_location)

    logging.info(f'Data saved to {output_file_location}')

def as_geojson(pl_data, input_dataframe_type: str, output_directory: str, output_file_name: str):
    # gdb_mineralsite = to_geopandas(df_input, input_dataframe_type)
    # # gdb_mineralsite = gdb_mineralsite[['source_id', 'record_id', 'location']]
    # gdb_mineralsite = gdb_mineralsite[['geometry']]
    df_processed_mineralsite = pl_data.to_pandas()
    gdb_mineralsite = gpd.GeoDataFrame(df_processed_mineralsite, 
                                       geometry=gpd.points_from_xy(df_processed_mineralsite.longitude, df_processed_mineralsite.latitude), 
                                       crs='EPSG:4326')

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    output_file_location = os.path.join(output_directory, output_file_name+'.geojson')

    gdb_mineralsite.to_file(output_file_location, driver='GeoJSON')

def as_json(pl_data, output_directory: str, output_file_name: str):
    """
    Stores the polars dataframe in a JSON file following the mineralsite schema
    Mineralsite schema diagram can be found here: https://github.com/DARPA-CRITICALMAAS/schemas/tree/main/ta2

    : param: pl_data = 
    : param: output_file_name = 
    """
    if 'latitude' in list(pl_data.columns) or 'longitude' in list(pl_data.columns):
        crs_value = get_epsg(pl_data.item(0, 'crs'))    # Convert CRS to EPSG value
        geometry = gpd.points_from_xy(pl_data['longitude'], pl_data['latitude'], crs=crs_value)
        list_geometry = [wkt.dumps(g, trim=True) for g in geometry.tolist()]

        pl_data = pl_data.drop(
            ['latitude', 'longitude', 'crs']
        ).with_columns(
            location = pl.Series(list_geometry),
            crs = pl.lit(crs_value)
        )
    
    attribute_record_identifiers = list(set(pl_data.columns) & set(['source_id', 'record_id', 'name', 'mineral_inventory', 'deposit_type_candidate']))
    attribute_location_info = list(set(pl_data.columns) & set(['location', 'crs', 'country', 'state_or_province']))

    pl_data = pl_data.select(
        pl.col(attribute_record_identifiers),
        location_info = pl.struct(pl.col(attribute_location_info)),
    )

    str_data = "{\"MineralSite\":" + pl_data.write_json(pretty=True, row_oriented=True) + "}"
    json_data = loads(str_data)
    cleaned_json_data = clean_nones(json_data)

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    output_file_location = os.path.join(output_directory, output_file_name+'.json')

    with open(output_file_location, 'w') as f:
        dump(cleaned_json_data, f, indent=4, default=str)

    del str_data, json_data, cleaned_json_data

def to_directory(list_pl_data, output_directory:str):
    for pl_data in list_pl_data:
        output_file_name = pl_data.item(0, 'source_id').replace('/', '_')
        output_file_location = os.path.join(output_directory, output_file_name+'.pkl')

        as_pkl(pl_data, output_file_location)