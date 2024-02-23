import os
import time
import logging
import argparse

import pickle5 as pickle
import regex as re
import polars as pl
import polars.selectors as cs
from sentence_transformers import SentenceTransformer, util

from m0_loading_and_saving.load_local_data import *
from m0_loading_and_saving.save_to_json_output import *

model = SentenceTransformer('all-MiniLM-L6-v2')

def identify_id_attribute(pl_mineralsite):
    """
    Identifies the attribute (column) that is representing the unique ID of the mineral site record.
    If there is no unique ID, it will provide a number for the mineral site

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
    
    """

    latitude_defintion = ""
    longitude_defintion = ""
    return 0

def identify_textlocation_attribute(pl_mineralsite):
    """
    Identifies the attribute that is representing the country and/or state of the mineral site

    : params: pl_mineralsite
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