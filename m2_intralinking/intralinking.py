import os
import time
import logging
import argparse

from m0_loading_and_saving.load_local_data import *
from m0_loading_and_saving.load_kg_data import *
from m2_intralinking.location_based_intralinking import *
from m2_intralinking.text_based_intralinking import *

logging.basicConfig(filename='fusemine.log', format='%(levelname)s:%(message)s', level=logging.INFO)

def main(args):
    if args.data_dir:
        print("need to load local data")
        print("need to convert it into mineral site schema format of data and upload it to the knowledge graph. need to be stored in the format of the dataframe so that it can be used by us")
    print("need to load kg data")

    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Linking mineral site within database and across databases')
    parser.add_argument('--data_dir', '-d',
                        help='directory in which the data files(.gdb, .csv, .geojson, .pkl, .json) and data dictionaries are saved')
    parser.add_argument('--use_location_base', '-l',
                        help='use location based method to link data', action='store_true')
    
    main(parser.parse_args())