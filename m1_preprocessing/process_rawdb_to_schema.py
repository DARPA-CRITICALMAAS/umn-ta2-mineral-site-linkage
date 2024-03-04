import os
import shutil

import time
import logging
import argparse
import configparser

import pickle5 as pickle
import polars as pl

from m0_loading_and_saving import load_local_data, save_to_geojson_output, save_to_json_output

from m1_preprocessing.extract_attributes_from_db import map_attribute_labels
# from m0_loading_and_saving.save_to_json_output import *
# from m0_loading_and_saving.save_to_geojson_output import save_mineralsite_output_geojson

# Initializing logging file for preprocessing
logging.basicConfig(filename='fusemine_preprocess.log', format='%(asctime)s - %(levelname)s:%(message)s', level=logging.INFO)

config = configparser.ConfigParser()
config.read('../params.ini')
path_params = config['directory.paths']

def create_locationinfo_schema(pl_mineralsite, dict_mapped_attributes):
    """
    
    : param: pl_mineralsite = 
    : param: dict_mapped_attributes = 
    : return: 
    """
    # pl_textual_data = pl_data.select(
    #     pl.col(list(dict_text_loc.keys())),
    #     crs = pl.lit(crs_value)
    # ).select(
    #     pl.when(pl.col(pl.Utf8).str.strip_chars() == "")
    #         .then(None)
    #         .otherwise(pl.col(pl.Utf8))
    #         .name.keep()
    # ).rename(dict_text_loc).to_pandas()

    # gpd_loc_info = pd.concat([gpd_geom, pl_textual_data], axis=1)
    # df_data = df_data.set_index('idx')
    # dict_data = df_data.to_dict('index')

    # tmp_data = pd.DataFrame(dict_data)
    # dict_data = {col: tmp_data[col].dropna().to_dict() for col in tmp_data}

    return dict_locationinfo

def create_mineralsite_schema(pl_mineralsite, dict_attributes):
    """
    
    : param: pl_mineralsite = polars dataframe of the raw mineralsite recrod
    : param dict_mapped_attributes = dictionary consisting of attribute name and definition
    : df_mineralsite = pandas dataframe with each mineralsite record formatted into the mineralsite schema
    """
    df_processed_mineralsite = pl_mineralsite.select(
        pl.col('source_id'),
        pl.col('record_id'),
        pl.col('name'),
        location_info = pl.struct(
            pl.col(['location', 'crs', 'country', 'state_or_province'])
        )
    ).to_pandas()
    
    return df_processed_mineralsite

def preprocessing_rawdb(list_mineralsite_sources, bool_geojson):
    """
    
    : param: list_mineralsite_sources = 
    : param: bool_geojson = 
    : return: list_preprocessed_mineralsites = 
    """
    raw_db_location = os.path.join(path_params['PATH_CHECKPOINT_DIR'], 'raw')
    preprocessed_location = os.path.join(path_params['PATH_CHECKPOINT_DIR'], 'preprocessed')

    list_preprocessed_mineralsites = []

    for source_name in list_mineralsite_sources:
        pl_mineralsite = load_local_data.open_local_files(raw_db_location, source_name, '.pkl')
        dict_attributes = load_local_data.open_local_files(raw_db_location, 'dict_'+source_name, '.pkl')

        pl_processed_mineralsite = map_attribute_labels(pl_mineralsite, dict_attributes)
        list_preprocessed_mineralsites.append(pl_processed_mineralsite)

        # Saving files to temporary (checkpoint) folder
        logging.info(f'\tSaving {source_name} data as JSON file to {preprocessed_location}')
        save_to_json_output.save_mineralsite_output_json(pl_processed_mineralsite, source_name, [preprocessed_location])

        if bool_geojson:
            logging.info(f'\tSaving {source_name} data as JSON file to {preprocessed_location}')
            with open(os.path.join(preprocessed_location, source_name+'.pkl'), 'wb') as handle:
                pickle.dump(pl_processed_mineralsite, handle, protocol=pickle.HIGHEST_PROTOCOL)

    return list_preprocessed_mineralsites

def main(args):
    logging.info(f'Preprocessing started')
    list_mineralsite_sources = load_local_data.open_local_directory(args.data_dir)
    list_preprocessed_mineralsites = preprocessing_rawdb(list_mineralsite_sources, args.save_as_geojson)

    logging.info(f'Preprocessing ended')

    # Extend the dictionary
    # a.update(b)

    # print("get the data that are on the knowledge graph, compare if any of the local data matches data already on record")

    # gdb_site_data = 0
    # file_name = args.commodity

    # json_file_location = ''
    # logging.info(f'\tSaving to JSON file at {json_file_location}')
    # print("save to json")

    # if args.save_as_geojson:
    #     geojson_file_location = ''
    #     logging.info(f'\tSaving to JSON file at {geojson_file_location}')

    #     save_mineralsite_output_geojson(gdb_site_data, file_name, list_path=['./'])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Converting raw database (e.g., MRDS, USMIN) to mineral site schema format')
    parser.add_argument('--data_dir', '-d',
                        help='directory in which the data files(.gdb, .csv, .geojson, .pkl, .json) and data dictionaries are saved')
    parser.add_argument('--save_as_geojson', '-g',
                        help='save output as geojson too', action='store_true')
    
    main(parser.parse_args())