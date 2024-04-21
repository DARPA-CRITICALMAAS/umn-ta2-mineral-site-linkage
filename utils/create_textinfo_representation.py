import os
import time
import logging
import configparser

import polars as pl
import pandas as pd

import torch
from sentence_transformers import SentenceTransformer

config = configparser.ConfigParser()
config.read('./params.ini')
text_params = config['text.params']

st_model = SentenceTransformer('sentence-transformers/all-mpnet-base-v2')

def text_embedding(input_str:str|list):
    if isinstance(input_str, list):
        input_str = ",".join(input_str)

    embedded_string = get_sentbert_embeddings(input_str)

    return embedded_string

def get_sentbert_embeddings(input_str:str):
    return st_model.encode(input_str)

def create_text_attribute_representation(pl_data):
    return 0

def row_to_json_string(pl_data):
    return 0

def row_to_doc_string(pl_data):
    return 0