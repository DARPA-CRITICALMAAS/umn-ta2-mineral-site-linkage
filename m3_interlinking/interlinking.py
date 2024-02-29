import os
import time
import logging
import argparse

from m0_loading_and_saving import load_local_data, load_kg_data, save_to_geojson_output, save_to_json_output
from m1_preprocessing import process_rawdb_to_schema
from m2_intralinking import intralinking

# Initializing logging file for preprocessing
logging.basicConfig(filename='fusemine_interlinking.log', format='%(levelname)s:%(message)s', level=logging.INFO)

def main(args):
    logging.info(f'Interlinking process started')

    if args.data_dir:
        list_mineralsite_sources = load_local_data.open_local_directory(args.data_dir)

    logging.info(f'Interlinking process ended')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Linking mineral site within database and across databases')
    parser.add_argument('--data_dir', '-d',
                        help='directory in which the data files(.gdb, .csv, .geojson, .pkl, .json) and data dictionaries are saved')
    
    main(parser.parse_args())