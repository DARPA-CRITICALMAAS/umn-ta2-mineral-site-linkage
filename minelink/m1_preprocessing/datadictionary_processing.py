import yake
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import regex as re
import pandas as pd

from sentence_transformers import SentenceTransformer, util
from minelink.m0_save_and_load.load_data import load_file

from minelink.params import *

xfrmer_model = 'all-MiniLM-L6-v2'

def create_short_description(description):
    kw_extractor = yake.KeywordExtractor(lan="end", n=4)

    return kw_extractor.extract_keywords(description)[0]

def df_to_dictionary(df):
    label = ''
    short = ''
    long = ''

    for c in df.columns.tolist():
        if re.search('label', c):
            label = c
            short = c
        elif re.search('short', c):
            short = c
        elif re.search('descri', c):
            long = c
        elif re.search('defin', c):
            long = c

    df = df.dropna(subset=[label])

    # TODO: deal with case when there is no column that consists of the word short

    df_description = df[[label, short, long]]
    df_description.columns = ['label', 'short', 'long']

    return df_description

def compare_dictionary(dict_target, dict_against):
    model = SentenceTransformer(xfrmer_model)

    name_target = list(dict_target.keys())
    descrip_target = list(dict_target.values())
    emb_target = model.encode(descrip_target, convert_to_tensor=True)

    name_against = list(dict_against.keys())
    descrip_against = list(dict_against.values())
    emb_against = model.encode(descrip_against, convert_to_tensor=True)

    cosine_scores = util.cos_sim(emb_target, emb_against)
    cosine_scores = np.array(cosine_scores.tolist())

    # sns.heatmap(cosine_scores, xticklabels=name_against, yticklabels=name_target, cmap="Blues")
    # plt.yticks(rotation=0) 
    # plt.savefig('heat.png')

    idx = list(dict.fromkeys(np.where(cosine_scores > 0.47)[1]))

    col_match = list(np.array(name_against)[idx])

    return col_match

def find_from_dictionary(df_dictionary, col_remaining, to_find):
    """
    :input: col_remaining (list) = 
    """
    dict_col_return = {key:[] for key in to_find}

    df_target = load_file(PATH_SRC_DIR, 'df_target', '.pkl')
    df_target = df_target[df_target['label'].isin(to_find)]
    dict_target = dict(zip(df_target['label'], df_target['description']))
    
    df_description = df_dictionary[df_dictionary['label'].isin(col_remaining)]

    if df_description.shape[0] > 0:
        dict_against = dict(zip(df_description['label'], df_description['long']))
        col_match = compare_dictionary(dict_target, dict_against)

    else:
        col_match = []

    # # TODO: call find_crs_from_description if and only if crs column is empty
    # if len(col_crs) == 0:
    #     crs_val = find_crs_from_description(description)

    return col_match