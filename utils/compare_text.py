import os
import time
import logging
import configparser

import ast
import polars as pl
from typing import Literal
from scipy import spatial
from itertools import combinations, product

import torch
from sentence_transformers import SentenceTransformer

from utils.dataframe_operations import *

config = configparser.ConfigParser()
config.read('./params.ini')
text_params = config['text.params']

st_model = SentenceTransformer('sentence-transformers/all-mpnet-base-v2')

def form_text_embeddings(input_str:str):
    return st_model.encode(input_str)

def measure_cosine_similarity(embedding1:str|list, embedding2:str|list, threshold_value:float, embedding_ratio:list|None=None) -> bool:
    if isinstance(embedding1, list):
        cosine_similarity = 1 - spatial.distance.cosine(form_text_embeddings(embedding1[0]), form_text_embeddings(embedding2[0]))
        # TODO: Need to deal with all components in embedding1

    cosine_similarity = 1 - spatial.distance.cosine(form_text_embeddings(embedding1), form_text_embeddings(embedding2))

    if cosine_similarity > threshold_value:
        return True
    return False

def compare_attribute_embedding():
    return 0

# Separate out the map element portion

def compare_text_embedding(list_pl_data, items_to_compare: list|None=None):
    if not items_to_compare:
        return pl_data

    list_indexes = list(range(pl_data.shape[0]))

    pl_data = pl_data.with_columns(
        pl.col(items_to_compare).map_elements(form_text_embeddings),
    )
    pl_data = add_index_columns(pl_data, 'tmp_index')

    # match orientation:
    #     case 'row':
            idx_combinations = combinations(list_indexes, 2)
            threshold_value = float(text_params['ATTRIBUTE_VALUE_THRESHOLD'])
            embedding_ratio = ast.literal_eval(text_params['ATTRIBUTE_VALUE_RATIO'])

        # case 'column':
        #     idx_combinations = product(list_indexes, repeat=2)
        #     threshold_value = float(text_params['ATTRIBUTE_DEFINITION_THRESHOLD'])

    # TODO: need to take into consideration where items to compare is more than 2
    list_bool_threshold = []
    for i in idx_combinations:
        str1 = pl_data.item(items_to_compare[0], i[0])
        str2 = pl_data.item(items_to_compare[1], i[1])

        list_bool_threshold.append(measure_cosine_similarity(str1, str2, threshold_value))