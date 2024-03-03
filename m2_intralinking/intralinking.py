import os
import time
import logging
import argparse
import configparser

from m0_loading_and_saving import load_local_data, load_kg_data, save_to_geojson_output, save_to_json_output
from m1_preprocessing.process_rawdb_to_schema import preprocessing_rawdb
from m2_intralinking.location_based_intralinking import *
from m2_intralinking.text_based_intralinking import *

# Initializing logging file for preprocessing
logging.basicConfig(filename='fusemine_intralinking.log', format='%(levelname)s:%(message)s', level=logging.INFO)

config = configparser.ConfigParser()
config.read('../params.ini')
path_params = config['directory.paths']

def intralinking(list_mineralsite_sources, bool_location_based):
    preprocessed_location = os.path.join(path_params['PATH_CHECKPOINT_DIR'], 'preprocessed')
    intralinked_location = os.path.join(path_params['PATH_CHECKPOINT_DIR'], 'intralinked')

    for source_name in list_mineralsite_sources:
        pl_mineralsite = load_local_data.open_local_files(preprocessed_location, source_name, '.pkl')

        logging.info(f'\tSaving intralinked {source_name} data as JSON file to {intralinked_location}')
    return 0

def main(args):
    logging.info(f'Intralinking process started')

    if args.data_dir:
        list_mineralsite_sources = load_local_data.open_local_directory(args.data_dir)
        preprocessing_rawdb(list_mineralsite_sources, False)

    print("need to load kg data")

    if not args.use_location_base:
        print("do text based linking")

    logging.info(f'Intralinking process ended')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Linking mineral site within database and across databases')
    parser.add_argument('--data_dir', '-d',
                        help='directory in which the data files(.gdb, .csv, .geojson, .pkl, .json) and data dictionaries are saved')
    parser.add_argument('--use_location_base', '-l',
                        help='use location based method to link data', action='store_true')
    
    main(parser.parse_args())