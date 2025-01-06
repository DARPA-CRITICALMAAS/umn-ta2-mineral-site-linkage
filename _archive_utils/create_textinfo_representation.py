import os
import time
import logging
import configparser

import random
import polars as pl
import pandas as pd

import torch
from sentence_transformers import SentenceTransformer

config = configparser.ConfigParser()
config.read('./params.ini')
text_params = config['text.params']

st_model = SentenceTransformer('sentence-transformers/all-mpnet-base-v2')

def text_embedding(input_str:str|list, bool_individual=False):
    if bool_individual & isinstance(input_str, list):
        input_str = ",".join(input_str)

    return get_sentbert_embeddings(input_str, not bool_individual)
    # return get_sentbert_embeddings(input_str).tolist()

def create_text_attribute_representation(pl_data):
    pl_data = pl_data.with_columns(
        combined_text = pl.struct()
    )
    
    return 0

def row_to_json_string(struct_input:dict, bool_shuffle:bool):
    list_headers = list(struct_input.keys())

    # If set to true, shuffle the order of the tabular text
    if bool_shuffle:
        random.shuffle(list_headers)

    json_formatted_str = '{'
    for h in list_headers:
        if struct_input[h] != '':
            json_formatted_str += f'{h}: {struct_input[h]}, '

    # Remove trailing comma and close brackets
    json_formatted_str = json_formatted_str.rstrip(', ')
    json_formatted_str += '}'

    return json_formatted_str

def row_to_doc_string(struct_input:dict, bool_shuffle:bool):
    list_headers = list(struct_input.keys())

    # If set to true, shuffle the order of the tabular text
    if bool_shuffle:
        random.shuffle(list_headers)

    doc_formatted_str = ''
    for h in list_headers:
        if struct_input[h] != '':
            doc_formatted_str += f'{h} is {struct_input[h]}. '

    return doc_formatted_str

def get_sentbert_embeddings(input_str:str, convert_to_tensor=True):
    return st_model.encode(input_str, convert_to_tensor=convert_to_tensor)