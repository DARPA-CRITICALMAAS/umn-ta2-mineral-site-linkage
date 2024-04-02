import os
import logging

import pickle
import polars as pl
import geopandas as gpd
from shapely import wkt
from json import loads, dump

def clean_nones(input_object: dict | list) -> dict | list:
    """
    Recursively remove all None values from either a dictionary or a list, and returns a new dictionary or list without the None values

    : param: input_object = either a dictionary or a list type that may or may not consist of a None value
    : return:= either a dictionary or a list type (same as the input) that does not consists of any None values
    """

    # List case
    if isinstance(input_object, list):
        return [clean_nones(x) for x in input_object if x is not None]
    
    # Dictionary case
    elif isinstance(input_object, dict):
        return {
            key: clean_nones(value)
            for key, value in input_object.items()
            if value is not None
        }

    else:
        return input_object

def as_csv(pl_data):
    return 0

def as_geojson(pl_data, output_directory: str, output_file_name: str):
    pl_processed_mineralsite = pl_processed_mineralsite.with_columns(
        pl.col('name').list.join(", ")
    )

    df_processed_mineralsite = pl_processed_mineralsite.to_pandas()
    gs_mineralsite = gpd.GeoSeries.from_wkt(df_processed_mineralsite['location'])
    gdb_mineralsite = gpd.GeoDataFrame(df_processed_mineralsite, geometry=gs_mineralsite, crs='EPSG:4326')

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
        crs_value = pl_data.item(0, 'crs')
        geometry = gpd.points_from_xy(pl_data['longitude'], pl_data['latitude'], crs=crs_value)
        list_geometry = [wkt.dumps(g, trim=True) for g in geometry.tolist()]

        pl_data = pl_data.with_columns(
            location = pl.Series(list_geometry)
        ).drop(
            ['latitude', 'longitude']
        )
    
    # pl_data.write_csv('./tmp.csv')

    attribute_deposit_type_candidate = ['deposit_type']
    attribute_mineral_inventory = ['commodity', 'observed_commodity']
    attribute_location_info = list(set(pl_data.column) & set(['location', 'crs', 'country', 'state']))

    pl_data = pl_data.select(
        location_info = pl.struct(pl.col(attribute_location_info))
    )

    # str_data = "{\"MineralSite\":" + pl_data.write_json(pretty=True, row_oriented=True) + "}"
    # json_data = loads(str_data)
    # cleaned_json_data = clean_nones(json_data)

    # if not os.path.exists(output_directory):
    #     os.makedirs(output_directory)
    # output_file_location = os.path.join(output_directory, output_file_name+'.json')

    # with open(output_file_location, 'w') as f:
    #     dump(cleaned_json_data, f, indent=4, default=str)

    # del str_data, json_data, cleaned_json_data