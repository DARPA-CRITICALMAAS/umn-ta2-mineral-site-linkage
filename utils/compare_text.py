import os
import time
import logging
import configparser

import polars as pl
from typing import Literal
from scipy import spatial
from itertools import combinations, product

import torch
from sentence_transformers import SentenceTransformer

from utils.tabular_operations import *

config = configparser.ConfigParser()
config.read('./params.ini')
geo_params = config['text.params']

st_model = SentenceTransformer('sentence-transformers/all-mpnet-base-v2')

def form_text_embeddings(input_str:str):
    return st_model.encode(input_str)

def measure_cosine_similarity(str_emb1, str_emb2, threshold_value:float, items_to_compare: list|None=None, embedding_ratio:float|None=None) -> bool:
    cosine_similarity = 1 - spatial.distance.cosine(str_emb1, str_emb2)

    # if embedding_ratio

    if cosine_similarity > threshold_value:
        return True
    return False

def compare_text(pl_data, orientation:Literal['row', 'column'], items_to_compare: list|None=None):
    if not items_to_compare:
        return pl_data

    list_indexes = list(range(pl_data.shape[0]))

    pl_data = pl_data.with_columns(
        pl.col(items_to_compare).map_elements(form_text_embeddings),
    )
    pl_data = add_index_columns(pl_data, 'tmp_index')

    match orientation:
        case 'row':
            idx_combinations = combinations(list_indexes, 2)

        case 'column':
            idx_combinations = product(list_indexes, repeat=2)