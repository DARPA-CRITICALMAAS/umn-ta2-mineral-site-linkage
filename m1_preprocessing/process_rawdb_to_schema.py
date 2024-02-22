import os
import time
import logging
import argparse

import regex as re
import polars as pl
import pickle5 as pickle

from m0_loading_and_saving.load_local_data import *
from m0_loading_and_saving.save_to_json_output import *

def identify_id_attribute(pl_mineralsite):
    """
    Identifies the attribute (column) that is representing the unique ID of the mineral site record.
    If there is no  unique ID, it will provide a number for the mineral site

    : params: pl_mineralsite
    """

    # TODO: The site name is considered to be unique for USMIN. Possibly make it purely rely on attribute identification?
    # Gives a dataframe that only has unique items
    pl_unique_column = pl_mineralsite.select(
        pl.all().unique().count() == pl_mineralsite.shape[0]
    )

    list_potential_id = [col.name for col in pl_unique_column if col.all()]

    if len(list_potential_id) == 0:     # Case where there is no column with all unique items
        return False
    elif len(list_potential_id) == 1:   # Caase where there is one column with all unique items
        return list_potential_id[0]
    
    # Case where there are multiple columns with all unique items
    for c in list_potential_id:
        splitted_col_name = re.split('[^A-Za-z]', c.lower())

        if 'id' in splitted_col_name:   # Select the attribute with the word 'id'
            return c

    return False

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

def identify_commodity_attribute():
    """
    
    """
    return 0

def main(args):
    # TODO: need to get all the comodities that are available on the minmod kg and compare that with the input  commodity
    if args.data_dir:
        print("read from the data directory and give an polars dataframe output with all the required characteristics")

    else:
        print("get the data that are on the knowledge graph")
    print(args.data_dir)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Converting raw database (e.g., MRDS, USMIN) to mineral site schema format')
    parser.add_argument('--data_dir', '-d',
                        help='directory in which the data files(.gdb, .csv, .geojson, .pkl, .json) and data dictionaries are saved')
    parser.add_argument('--commodity', '-c',
                        help='mineral site commodity for which needs to be reconciled')
    
    main(parser.parse_args())