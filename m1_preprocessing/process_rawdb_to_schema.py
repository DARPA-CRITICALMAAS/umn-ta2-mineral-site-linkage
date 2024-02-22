import os
import time
import logging
import argparse

import polars as pl
import pickle5 as pickle

from m0_loading_and_saving.load_local_data import *
from m0_loading_and_saving.save_to_json_output import *

def identify_id_attribute():
    """
    
    """
    return 0

def identify_name_attribute():
    """

    """

    name_definition = ""
    other_name_definition = ""

    return 0

def identify_geolocation_attribute():
    """
    
    """

    latitude_defintion = ""
    longitude_defintion = ""
    return 0

def identify_textlocation_attribute():
    """
    
    """
    return 0

def main(args):
    args.data_dir
    print("main")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Converting raw database (e.g., MRDS, USMIN) to mineral site schema format')
    parser.add_argument('--data_dir', '-d',
                        help='directory in which the data files(.gdb, .csv, .geojson, .pkl, .json) and data dictionaries are saved')
    
    main(parser.parse_args())