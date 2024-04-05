import os
import time
import logging
import configparser

import ast
import polars as pl
from typing import Literal
from scipy import spatial
from itertools import combinations, product

from utils.dataframe_operations import *
from utils.create_textinfo_representation import *

config = configparser.ConfigParser()
config.read('./params.ini')
text_params = config['text.params']

def measure_cosine_similarity(embedding1:list, embedding2:list, threshold_value:float) -> bool:
    cosine_similarity = 1 - spatial.distance.cosine(embedding1, embedding2)

    if cosine_similarity > threshold_value:
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

def compare_text_value_embedding(list_pl_data, items_to_compare:list|None=None):
    # No text based comparison
    if not items_to_compare:
        return list_pl_data

    for pl_data in list_pl_data:
        pl_data = pl_data.with_columns(
            pl.col(items_to_compare).map_elements(lambda x: text_embedding(x))
        )
        pl_data = add_index_columns(pl_data, 'GroupID')

        # Create combinations of index of every row
        list_indexes = list(range(pl_data.shape[0]))
        idx_combinations = combinations(list_indexes, 2)
        threshold_value = float(text_params['ATTRIBUTE_VALUE_THRESHOLD'])

        for tuple_combination in idx_combinations:
            str1 = pl_data.item(items_to_compare[0], tuple_combination[0])
            str2 = pl_data.item(items_to_compare[1], tuple_combination[1])

            if measure_cosine_similarity(str1, str2, threshold_value):
                pl_data[tuple_combination[0], 'GroupID'] = pl_data[tuple_combination[1], 'GroupID']

        pl_data = pl_data.group_by('GroupID').agg([pl.all()])

    return list_pl_data