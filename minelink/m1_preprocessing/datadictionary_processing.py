import yake
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import regex as re
import pandas as pd

from sentence_transformers import SentenceTransformer, util
from minelink.m0_save_and_load.save_load_file import load_file

xfrmer_model = 'all-MiniLM-L6-v2'

def create_short_description(description):
    kw_extractor = yake.KeywordExtractor(lan="end", n=4)

    return kw_extractor.extract_keywords(description)[0]

def df_to_dictionary(df):
    label = ''
    short = ''
    long = ''

    for c in df.columns.tolist():
        if c == 'label':
            label = c
        elif re.search('short', c):
            short = c
        elif re.search('descri', c):
            long = c

    df = df.dropna(subset=[label])

    # TODO: deal with case when there is no column that consists of the word short

    df_description = df[[label, short, long]]
    df_description.columns = ['label', 'short', 'long']

    return df_description

def compare_description_list(df_description_first, df_description_second):
    model = SentenceTransformer(xfrmer_model)


    # TODO: grab the long description from both dataframe and compare with the method below

    return 0 # string of description of latitude

def dictionary_available(dict_description):
    model = SentenceTransformer(xfrmer_model)

    dict_target = load_file('./src', 'target', '.pkl')

    name_target = list(dict_target.keys())
    descrip_target = list(dict_target.values())
    emb_target = model.encode(descrip_target, convert_to_tensor=True)

    name_against = list(dict_description.keys())
    descrip_against = list(dict_description.values())
    emb_against = model.encode(descrip_against, convert_to_tensor=True)

    cosine_scores = util.cos_sim(emb_target, emb_against)
    cosine_scores = np.array(cosine_scores.tolist())

    sns.heatmap(cosine_scores, xticklabels=name_against, yticklabels=name_target, cmap="Blues")
    plt.yticks(rotation=0) 
    plt.savefig('heat.png')

    idx = list(dict.fromkeys(np.where(cosine_scores > 0.47)[1]))

    col_merge = []
    col_remove = list(np.array(name_against)[idx])

    return col_remove

def find_crs_from_description(description):
    list_crs = load_file('./minelink/src', 'crs', '.pkl')

    list_tokens = re.split(' ', description)

    for token in list_tokens:
        if token in list_crs:
            return token
        
    return 'WGS84'  # Return default if there does not exists a crs value in the data

def find_from_dictionary(df_dictionary, col_remaining, to_find):
    list_col_return = []

    print(len(to_find), to_find)

    # col_latitude = []
    # col_longitude = []
    # col_crs = []

    # description = 'latitude is the decimal value in wgs84'


    # # TODO: call find_crs_from_description if and only if crs column is empty
    # if len(col_crs) == 0:
    #     crs_val = find_crs_from_description(description)

    crs_val = 'WGS84'

    return list_col_return, crs_val