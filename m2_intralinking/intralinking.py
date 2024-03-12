import os
import sys
import time
import logging
import pickle
import argparse
import configparser

# System path configuration for relative imports
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from m0_loading_and_saving import load_local_data, load_kg_data, save_to_geojson_output, save_sameas_output
from m1_preprocessing.process_rawdb_to_schema import preprocessing_rawdb
from m2_intralinking.location_based_intralinking import location_based_linking
from m2_intralinking.text_based_intralinking import text_based_linking

# Initializing logging file for intralinking
logging.basicConfig(filename='fusemine_intralinking.log', format='%(levelname)s:%(message)s', level=logging.INFO)

config = configparser.ConfigParser()
config.read('../params.ini')
path_params = config['directory.paths']

def intralinking(list_mineralsite_sources, bool_location_based, bool_geojson):
    preprocessed_location = os.path.join(path_params['PATH_CHECKPOINT_DIR'], 'preprocessed')
    intralinked_location = os.path.join(path_params['PATH_CHECKPOINT_DIR'], 'intralinked')

    for source_name in list_mineralsite_sources:
        pl_mineralsite = load_local_data.open_local_files(preprocessed_location, source_name, '.pkl')
        pl_intralinked_mineralsite = location_based_linking(pl_mineralsite)

        if not bool_location_based:
            pl_intralinked_mineralsite = text_based_linking(pl_intralinked_mineralsite)

        logging.info(f'\tSaving linked {source_name} data as two column CSV file to {intralinked_location}')
        save_sameas_output.save_sameas_output_csv(pl_intralinked_mineralsite, intralinked_location, source_name)

        if bool_geojson:
            logging.info(f'\tSaving {source_name} data as GEOJSON file to {intralinked_location}')
            save_to_geojson_output.save_mineralsite_output_geojson(pl_intralinked_mineralsite, intralinked_location, source_name)

def main(args):
    if args.data_dir:
        list_mineralsite_sources = load_local_data.open_local_directory(args.data_dir)
        preprocessing_rawdb(list_mineralsite_sources, False)

    # Need to load KG data somewhere here

    logging.info(f'Intralinking process started')
    start_time = time.time()

    intralinking(list_mineralsite_sources, args.use_location_base, args.save_as_geojson)

    logging.info(f'Intralinking process ended: Total run time: {time.time() - start_time}s')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Linking mineral site within database and across databases')
    parser.add_argument('--data_dir', '-d',
                        help='directory in which the data files(.gdb, .csv, .geojson, .pkl, .json) and data dictionaries are saved')
    parser.add_argument('--use_location_base', '-l',
                        help='use location based method to link data', action='store_true')
    parser.add_argument('--save_as_geojson', '-g',
                        help='save output as geojson too', action='store_true')
    
    main(parser.parse_args())