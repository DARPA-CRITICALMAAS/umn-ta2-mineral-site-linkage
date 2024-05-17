import os
import time
import logging
import configparser

import torch
import torch.nn as nn
from torchmetrics.functional.pairwise import pairwise_cosine_similarity
from tqdm import tqdm

import ast
import regex as re
import numpy as np
import polars as pl
from scipy import spatial
from itertools import combinations, product

from utils.load_files import *
from utils.dataframe_operations import *
from utils.create_textinfo_representation import *

device = 'cuda' if torch.cuda.is_available() else 'cpu'

config = configparser.ConfigParser()
config.read('./params.ini')
text_params = config['text.params']
path_params = config['directory.paths']

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

def measure_cosine_similarity_cpu(embedding1:list, embedding2:list, embedding3:list, embedding4:list, threshold_value:float) -> bool:
    cosine_similarity1 = 1 - spatial.distance.cosine(embedding1, embedding2)
    cosine_similarity2 = 1 - spatial.distance.cosine(embedding3, embedding4)

    # TODO: dimension reduction
    if 0.79 * cosine_similarity1 + 0.21 * cosine_similarity2 > threshold_value:
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

        if measure_cosine_similarity_cpu(def_target_attribute, def_db_attribute, text_params['ATTRIBUTE_DEFINITION_THRESHOLD']):
            identified_attribute[i[1]] = i[0]

    return identified_attribute

def compare_text_embedding(pl_data, source_id:str|None=None, items_to_compare:list|None=None):
    if not source_id:
        source_id = 'ALL'
    
    if 'GroupID' not in list(pl_data.columns):
        pl_data = add_index_columns(pl_data=pl_data,
                                    index_column_name='GroupID')
        
    if not items_to_compare or pl_data.shape[0] < 2:
        pl_data = pl_data.with_columns(
            GroupID_text = pl.lit(source_id) + pl.lit('default')
        ).drop('GroupID')

        return pl_data
    
    suffix_regex = f"(?i){initiate_load(os.path.join(path_params['PATH_RSRC_DIR'], 'site_suffix.pkl'))}"

    pl_data = pl_data.with_columns(
        pl.col(items_to_compare).str.strip_chars().str.replace(rf"{suffix_regex}", '')
    )

    if device == 'cpu':
        return compare_text_value_embedding_cpu(pl_data, source_id, items_to_compare)

    else:
        return compare_text_value_embedding_cuda(pl_data, source_id, items_to_compare)

def compare_text_value_embedding_cuda(pl_data, source_id:str|None=None, items_to_compare:list|None=None):
    list_attribute_ratio = ast.literal_eval(text_params['ATTRIBUTE_VALUE_RATIO'])
    default_ratio = float(text_params['DEFAULT_WEIGHT_FACTOR'])

    list_embedding_matrix = []
    for idx, item in enumerate(items_to_compare):
        list_item_values = pl_data[item].to_list()

        start_time = time.time()
        list_item_embedding = text_embedding(list_item_values)

        start_time = time.time()
        similarity_score = pairwise_cosine_similarity(list_item_embedding).numpy(force=True)
        similarity_score = np.triu(similarity_score)
        try:
            normalized_similarity = list_attribute_ratio[idx] * similarity_score
        except:
            normalized_similarity = default_ratio * similarity_score

        if idx == 0:
            list_embedding_matrix = normalized_similarity
        else:
            list_embedding_matrix = list_embedding_matrix + normalized_similarity
    
    list_condition_satisfied = np.transpose(np.nonzero(list_embedding_matrix > float(text_params['ATTRIBUTE_VALUE_THRESHOLD'])))
    for idx, pair in enumerate(tqdm(list_condition_satisfied)):
        if source_id == 'ALL' and pl_data[int(pair[1]), 'source_id'] == pl_data[int(pair[0]), 'source_id']:
            continue

        pl_data[int(pair[1]), 'GroupID'] = pl_data[int(pair[0]), 'GroupID']

    duplicates = pl_data.group_by(
        'record_id'
    ).agg([pl.all()]).filter(
        pl.col('GroupID').list.len() > 1
    ).select(
        to_col = pl.col('GroupID').list.get(0),
        from_col = pl.col('GroupID')
    ).explode('from_col').filter(
        pl.col('to_col') != pl.col('from_col')
    )
    dictionary_duplicates = as_dictionary(duplicates, 'from_col', 'to_col')

    pl_data = pl_data.with_columns(
        pl.col('GroupID').replace(dictionary_duplicates)
    ).unique(
        'record_id',
        keep='first'
    )

    logging.info(f'\t\tText embeddings compared - Elapsed time: {time.time() - start_time}')

    pl_data = pl_data.with_columns(
        GroupID_text = pl.lit(source_id) + pl.col('GroupID').cast(pl.Utf8)
    ).drop('GroupID')

    return pl_data

def compare_text_value_embedding_cpu(pl_data, source_id:str|None=None, items_to_compare:list|None=None):
    start_time = time.time()
    pl_data = pl_data.with_columns(
        pl.col(items_to_compare).map_elements(lambda x: text_embedding(x)).name.map(lambda c: c+'_embedding')
    )

    # Create combinations of index of every row
    list_indexes = list(range(pl_data.shape[0]))
    idx_combinations = combinations(list_indexes, 2)
    threshold_value = float(text_params['ATTRIBUTE_VALUE_THRESHOLD'])

    for tuple_combination in tqdm(idx_combinations):
        str1 = pl_data.item(tuple_combination[0], items_to_compare[0]+'_embedding')
        str2 = pl_data.item(tuple_combination[1], items_to_compare[0]+'_embedding')

        str3 = pl_data.item(tuple_combination[0], items_to_compare[1]+'_embedding')
        str4 = pl_data.item(tuple_combination[1], items_to_compare[1]+'_embedding')

        if measure_cosine_similarity_cpu(str1, str2, str3, str4, threshold_value):
            pl_data[tuple_combination[1], 'GroupID'] = pl_data[tuple_combination[0], 'GroupID']
    
    logging.info(f'\t\tText embeddings compared - Elapsed time: {time.time() - start_time}')

    pl_data = pl_data.with_columns(
        GroupID_text = pl.lit(source_id) + pl.col('GroupID').cast(pl.Utf8)
    ).drop(
        ['GroupID', items_to_compare[0]+'_embedding', items_to_compare[1]+'_embedding']
    )

    return pl_data