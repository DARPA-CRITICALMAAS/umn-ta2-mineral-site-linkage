import os
import time
import logging
import configparser

import ast
import regex as re
import polars as pl
from typing import Literal
from scipy import spatial
from itertools import combinations, product

from utils.dataframe_operations import *
from utils.create_textinfo_representation import *

config = configparser.ConfigParser()
config.read('./params.ini')
text_params = config['text.params']

def string_match_on_attribute(pl_data, item_to_string_match:str):
    item_to_string_match = re.sub('[^A-Za-z0-9]', '', item_to_string_match)

    pl_data = pl_data.with_columns(
        pl.col(pl.Utf8).str.strip_chars()
    ).with_columns(
        all_combined = pl.concat_str(
            pl.all().fill_null('').str.replace_all("[^A-Za-z0-9]", "")           
        )
    ).filter(
        pl.col('all_combined').str.contains(rf'(?i){item_to_string_match}')
    )

    return pl_data

def measure_cosine_similarity(embedding1:list, embedding2:list, embedding3:list, embedding4:list, threshold_value:float) -> bool:
    cosine_similarity1 = 1 - spatial.distance.cosine(embedding1, embedding2)
    cosine_similarity2 = 1 - spatial.distance.cosine(embedding3, embedding4)
    # TODO: dimension reduction

    if 0.79 * cosine_similarity1 + 0.21 * cosine_similarity2> threshold_value:
        return True
    return False

def compare_attribute_embedding(target_attribute_dictionary:dict, db_attribute_dictionary:dict) -> dict:
    target_attribute_dictionary = {k: text_embedding(v) for k, v in target_attribute_dictionary.items}
    db_attribute_dictionary = {k: text_embedding(v) for k, v in db_attribute_dictionary.items}

    list_target_attributes = list(target_attribute_dictionary.keys())
    list_db_attributes = list(db_attribute_dictionary.keys())

    combinations_to_compare = product(list_target_attributes, list_db_attributes)

    identified_attribute = {}
    for i in combinations_to_compare:
        def_target_attribute = target_attribute_dictionary[i[0]]
        def_db_attribute = db_attribute_dictionary[i[1]]

        if measure_cosine_similarity(def_target_attribute, def_db_attribute, text_params['ATTRIBUTE_DEFINITION_THRESHOLD']):
            identified_attribute[i[1]] = i[0]

    return identified_attribute

def compare_text_value_embedding(pl_data, source_id:str|None=None, items_to_compare:list|None=None):
    if not source_id:
        source_id = 'ALL'
        
    if not items_to_compare:
        return pl_data
    
    logging.info(f'\t\ttext linking with: {", ".join(items_to_compare)}')
    
    pl_data = pl_data.with_columns(
        pl.col(items_to_compare).map_elements(lambda x: text_embedding(x)).name.map(lambda c: c+'_embedding')
    )
    pl_data = add_index_columns(pl_data, 'GroupID')

    # Create combinations of index of every row
    list_indexes = list(range(pl_data.shape[0]))
    idx_combinations = combinations(list_indexes, 2)
    threshold_value = float(text_params['ATTRIBUTE_VALUE_THRESHOLD'])

    for tuple_combination in idx_combinations:
        str1 = pl_data.item(tuple_combination[0], items_to_compare[0]+'_embedding')
        str2 = pl_data.item(tuple_combination[1], items_to_compare[0]+'_embedding')

        str3 = pl_data.item(tuple_combination[0], items_to_compare[1]+'_embedding')
        str4 = pl_data.item(tuple_combination[1], items_to_compare[1]+'_embedding')

        if measure_cosine_similarity(str1, str2, str3, str4, threshold_value):
            pl_data[tuple_combination[1], 'GroupID'] = pl_data[tuple_combination[0], 'GroupID']
    
    pl_data = pl_data.with_columns(
        GroupID_text = pl.lit(source_id) + pl.col('GroupID').cast(pl.Utf8)
    ).drop(
        ['GroupID', items_to_compare[0]+'_embedding', items_to_compare[1]+'_embedding']
    )

    return pl_data