import seaborn as sns
import matplotlib.pyplot as plt

import regex as re
import yake

import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer, util

from minelink.params import *
from minelink.m0_SaveAndLoad.save_load_file import load_file

xfrmer_model = 'all-MiniLM-L6-v2'

def df_to_dict(df_data_dic):
    dict_description = df_data_dic
    return dict_description

def find_crs(list_sent_geo):
    """

    input: list_sent_geo = list of sentences that describes latitude and longitudes
    return: crs = string representing the CRS value (either in CRS or EPSG)
    """

    for item in list_sent_geo:
        crs = 'NAD83'

        return crs
    
    return 'WGS84'  # If cannot find an acceptable form of crs just return WGS84

def find_name_column(columns, bool_dict):
    if bool_dict == False:
        list_commonly_used = ['depname', 'dep_name', 'site_name', 'names', 'name', 'other_name', 'name_other', 'ftr_name']
        return 0
    
    return 0

def find_name_geom_columns(df_data, df_dic):
    """
    input: df_data = dataframe 
    input: df_dic = dataframe
    """
    # dict_description = df_to_dict(df_dic)

    try:
        crs = df_data.crs
        lat = df_data.geometry.y
        long = df_data.geometry.x

        # If there are any columns matching the values of crs, lat, long drop them from df_dic

        bool_geometry = True
        
    except:
        # Convert dictionary dataframe into form of dictionary and convert them to BERT embeddings
        print("None")
        bool_geometry = False

    col_name = 'name'

    # Needs to be longitude, latitude, crs
    list_col_geom = [long, lat, crs]

    selection_threshold = 0.4

    return col_name, list_col_geom, bool_geometry

def dictionary_unavailable(df_data):
    col_data = df_data.columns

    for c in col_data:
        if re.search("^LAT", c.upper()):
            print(c)
        elif re.search("^LONG", c.upper()):
            print(c)
        elif c.upper() == 'CRS':
            print(c)

    return 0

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

def find_columns(df, source_alias_code):
    # df_dictionary = load_file(PATH_TMP_DIR+source_alias_code, 'dictionary', '.pkl')

    # [col_site_name, col_other_names], [col_latitude], [col_longitude], [col_crs], col_state_province, [unique_id]

    return 0