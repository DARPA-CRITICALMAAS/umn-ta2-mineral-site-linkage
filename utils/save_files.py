import os
import logging
import configparser

import pickle
import polars as pl
import geopandas as gpd
from shapely import wkt
from json import loads, dump
from itertools import product
from datetime import datetime, timezone

from utils.unify_coordinate_system import *
from utils.convert_dataframe import *
from utils.dataframe_operations import *
from utils.load_files import *

config = configparser.ConfigParser()
config.read('./params.ini')
path_params = config['directory.paths']

def create_directory(directory_path:str):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

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
        
        list_groups = pl_data['ms_uri'].to_list()
        
        list_uris_combination = []

        for i in list_groups:
            list_uris_combination.extend(product(i, repeat=2))

        tmp_pd = pd.DataFrame(list_uris_combination, columns=['ms_uri_1', 'ms_uri_2'])
        pl_data = pl.from_pandas(tmp_pd)

    pl_data = pl_data.with_columns(
        pl.col(['ms_uri_1', 'ms_uri_2']).str.replace('https://minmod.isi.edu/resource/', '')
    )
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
        # pl_value_map = initiate_load(os.path.join(path_params['PATH_MAPFILE_DIR'], path_params['PATH_CRS_MAPFILE']))
        # dict_epsg = as_dictionary(pl_value_map, 'name', 'minmod_id')

        # crs_value = get_epsg(pl_data.item(0, 'crs'))    # Convert CRS to EPSG value
        # print(crs_value)
        crs_value = pl_data.item(0, 'crs')['observed_name']
        pl_data = pl_data.with_columns(
            pl.col(['latitude', 'longitude']).cast(pl.Float64)
        )
        geometry = gpd.points_from_xy(pl_data['longitude'], pl_data['latitude'], crs=crs_value)
        list_geometry = [wkt.dumps(g, trim=True) for g in geometry.tolist()]

        # print(crs_value)
        # crs_value = dict_epsg[crs_value]
        # print(crs_value)

        pl_data = pl_data.drop(
            ['latitude', 'longitude']
        ).with_columns(
            location = pl.Series(list_geometry),
        )
    
    todays_date = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    pl_data = pl_data.with_columns(
        pl.col('reference').map_elements(lambda x: [x]),
        modified_at = pl.lit(todays_date),
    )
    attribute_record_identifiers = list(set(pl_data.columns) & set(['source_id', 'record_id', 'name', 'aliases', 'mineral_inventory', 'deposit_type_candidate', 'site_type', 'modified_at', 'created_by', 'reference']))
    attribute_location_info = list(set(pl_data.columns) & set(['location', 'crs', 'country', 'state_or_province']))

    pl_data = pl_data.select(
        pl.col(attribute_record_identifiers),
        location_info = pl.struct(pl.col(attribute_location_info)).map_elements(lambda x: data_to_none(input_object=x, col_decision='location', col_affected='crs', condition='POINT EMPTY')),
    )

    str_data = pl_data.write_json()
    json_data = loads(str_data)
    cleaned_json_data = clean_nones(clean_nones(clean_nones(json_data)))

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    output_file_location = os.path.join(output_directory, f'{output_file_name}.json')

    with open(output_file_location, 'w') as f:
        dump(cleaned_json_data, f, indent=4, default=str)

    del str_data, json_data, cleaned_json_data

    logging.info(f'File saved to {output_directory} as {output_file_name}.json')

def to_directory(list_pl_data, output_directory:str):
    for pl_data in list_pl_data:
        output_file_name = pl_data.item(0, 'source_id').replace('/', '_')
        output_file_location = os.path.join(output_directory, output_file_name+'.pkl')

        as_pkl(pl_data, output_file_location)