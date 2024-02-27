import os
import time
import logging
import argparse
import configparser

import pickle5 as pickle
import regex as re
import polars as pl
import polars.selectors as cs
from sentence_transformers import SentenceTransformer, util

from m0_loading_and_saving.load_local_data import open_local_directory
from m0_loading_and_saving.save_to_json_output import *
from m0_loading_and_saving.save_to_geojson_output import save_mineralsite_output_geojson

# Initializing logging file for preprocessing
logging.basicConfig(filename='fusemine_preprocess.log', format='%(levelname)s:%(message)s', level=logging.INFO)
model = SentenceTransformer('all-MiniLM-L6-v2')

def identify_id_attribute(pl_mineralsite):
    """
    Identifies the attribute (column) that is representing the unique ID of the mineral site record.
    If there is no unique ID, it will provide a number for the mineral site

    : param: pl_mineralsite = 
    : return: 
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

def identify_name_attribute(pl_mineralsite):
    """
    Identifies the attribute that is representing the name and other name (i.e., name alias) of the mineral site
    If there are no name, it will be named 'Unknown'

    : params: pl_mineralsite
    """
    name_definition = ""

    # Selects columns that has 'string' type attribute
    pl_alpha_data = pl_mineralsite.select(
        ~cs.by_dtype(pl.NUMERIC_DTYPES, pl.Boolean)
    )
    list_alpha_attribute = pl_alpha_data.columns


    # pl_name = pl_data.select(
    #     pl.col(remaining_columns)
    # ).select(
    #     ~cs.by_dtype(pl.NUMERIC_DTYPES, pl.Boolean)
    # )

    # col_match = compare_description(dict_data, dict_target, potential_name, ['name'])
    
    # # Will be deleted
    # potential = set(remaining_columns) & set(['Ftr_Name', 'Site_Name', 'site_name', 'Site'])
    # potential = set(col_match) | potential
    
    # return list(potential)

    # name_target = list(dict_target.keys())
    # descrip_target = list(np.array(list(dict_target.values())).flatten())
    # emb_target = model.encode(descrip_target, convert_to_tensor=True)

    # name_against = list(dict_against.keys())
    # descrip_against = list(np.array(list(dict_against.values())).flatten())
    # emb_against = model.encode(descrip_against, convert_to_tensor=True)

    # cosine_scores = util.cos_sim(emb_target, emb_against)
    # cosine_scores = np.array(cosine_scores.tolist())

    # idx = list(dict.fromkeys(np.where(cosine_scores > 0.45)[1]))

    # col_match = np.array(name_against)[idx]

    # return col_match

    # name_definition = ""
    # other_name_definition = ""

def identify_geolocation_attribute(pl_mineralsite):
    """
    Identifies the attribute that is representing the latitude and longitude of the mineral site
    If there is no attributes representing such, it will return a None item

    : params: pl_mineralsite: polars dataframe of all the records from the mineral site data
    : return: dict_text_loc: dictionary consisting
    """

    latitude_defintion = ""
    longitude_defintion = ""
    return 0

def identify_textlocation_attribute(pl_mineralsite):
    """
    Identifies the attribute that is representing the country and/or state of the mineral site

    : params: pl_mineralsite: polars dataframe of all the records from the mineral site data
    : return: dict_text_loc: dictionary consisting
    """
    dict_text_loc = {}

    for i in list(pl_mineralsite.columns):
        if i.lower() == 'country':
            dict_text_loc[i] = 'country'
        elif re.search('state', i.lower()) or re.search('province', i.lower()):
            dict_text_loc[i] = 'state_or_province'

    return dict_text_loc

def identify_commodity_attribute(pl_mineralsite):
    """
    
    """
    commodity_defintion = ""
    return 0

def main(args):
    logging.info(f'Preprocessing started')

    if args.data_dir:
        print("read from the data directory and give an polars dataframe output with all the required characteristics")
        open_local_directory()

    else:
        print("get the data that are on the knowledge graph")
    print(args.data_dir)

    gdb_site_data = 0
    file_name = args.commodity

    json_file_location = ''
    logging.info(f'\tSaving to JSON file at {json_file_location}')
    print("save to json")

    if args.save_as_geojson:
        geojson_file_location = ''
        logging.info(f'\tSaving to JSON file at {geojson_file_location}')

        save_mineralsite_output_geojson(gdb_site_data, file_name, list_path=['./'])

    logging.info(f'Preprocessing ended')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Converting raw database (e.g., MRDS, USMIN) to mineral site schema format')
    parser.add_argument('--data_dir', '-d',
                        help='directory in which the data files(.gdb, .csv, .geojson, .pkl, .json) and data dictionaries are saved')
    parser.add_argument('--commodity', '-c',
                        help='mineral site commodity for which needs to be reconciled')
    parser.add_argument('--save_as_geojson', '-g',
                        help='save output as geojson too', action='store_true')
    
    main(parser.parse_args())