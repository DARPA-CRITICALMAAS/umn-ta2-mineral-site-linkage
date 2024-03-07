import os
import time
import logging
import argparse

from m0_loading_and_saving.load_local_data import open_local_directory
from m1_preprocessing.extract_attributes_from_db import map_attribute_labels
# logging.basicConfig(filename='fusemine.log', format='%(levelname)s:%(message)s', level=logging.INFO)

def main(args):
    # list_mineralsite_sources = open_local_directory(args.data_dir)
    print("main")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Linking mineral site within database and across databases')
    parser.add_argument('--data_dir', '-d',
                        help='directory in which the data files(.gdb, .csv, .geojson, .pkl, .json) and data dictionaries are saved')
    parser.add_argument('--output_filename', '-o',
                        help='final output filename of the interlinked result')
    parser.add_argument('--use_location_base', '-l',
                        help='use location based method to link data', action='store_true')
    parser.add_argument('--save_as_geojson', '-g',
                        help='save output as geojson too', action='store_true')
    parser.add_argument('--module', '-m',
                        help='m1 for preprocessing; m2 for intralinking; m3 for interlinking')
    
    main(parser.parse_args())