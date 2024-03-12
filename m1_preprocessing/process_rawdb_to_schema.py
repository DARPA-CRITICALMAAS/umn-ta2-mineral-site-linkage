import os
import sys
import shutil
import time
import logging
import argparse
import configparser

import pickle
import polars as pl

# System path configuration for relative imports
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from m0_loading_and_saving import load_local_data, save_to_geojson_output, save_to_json_output
from m1_preprocessing.extract_attributes_from_db import map_attribute_labels

config = configparser.ConfigParser()
config.read('../params.ini')
# print(config.sections())
path_params = config['directory.paths']

# Initializing logging file for preprocessing
logging.basicConfig(filename='fusemine_preprocess.log', format='%(asctime)s - %(levelname)s:%(message)s', level=logging.INFO)

def preprocessing_rawdb(list_mineralsite_sources, bool_geojson):
    """
    Initiates the raw database preprocessing process

    : param: list_mineralsite_sources = list of the source name of the mineral site databases
    : param: bool_geojson = boolean (T/F) value to indicate storing in geojson format
    : return: list_preprocessed_mineralsites = list of processed dataframes
    """
    raw_db_location = os.path.join(path_params['PATH_CHECKPOINT_DIR'], 'raw')
    preprocessed_location = os.path.join(path_params['PATH_CHECKPOINT_DIR'], 'preprocessed')

    list_preprocessed_mineralsites = []

    for source_name in list_mineralsite_sources:
        pl_mineralsite = load_local_data.open_local_files(raw_db_location, source_name, '.pkl')
        dict_attributes = load_local_data.open_local_files(raw_db_location, 'dict_'+source_name, '.pkl')

        pl_processed_mineralsite = map_attribute_labels(pl_mineralsite, dict_attributes).with_columns(
            source_id = pl.lit(source_name)
        )
        list_preprocessed_mineralsites.append(pl_processed_mineralsite)

        # Saving files to temporary (checkpoint) folder
        logging.info(f'\tSaving {source_name} data as JSON file to {preprocessed_location}')
        save_to_json_output.save_mineralsite_output_json(pl_processed_mineralsite, preprocessed_location, source_name)

        if bool_geojson:
            logging.info(f'\tSaving {source_name} data as GEOJSON file to {preprocessed_location}')
            save_to_geojson_output.save_mineralsite_output_geojson(pl_processed_mineralsite, preprocessed_location, source_name)

    return list_preprocessed_mineralsites

def main(args):
    logging.info(f'Preprocessing started')
    list_mineralsite_sources = load_local_data.open_local_directory(args.data_dir)
    list_preprocessed_mineralsites = preprocessing_rawdb(list_mineralsite_sources, args.save_as_geojson)

    # # TODO: Load the data from knowledge graph and see whether the data is already existing in the knowledge graph (need to skip those)
    logging.info(f'Preprocessing ended')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Converting raw database (e.g., MRDS, USMIN) to mineral site schema format')
    parser.add_argument('--data_dir', '-d',
                        help='directory in which the data files(.gdb, .csv, .geojson, .pkl, .json) and data dictionaries are saved')
    parser.add_argument('--save_as_geojson', '-g',
                        help='save output as geojson too', action='store_true')
    
    main(parser.parse_args())