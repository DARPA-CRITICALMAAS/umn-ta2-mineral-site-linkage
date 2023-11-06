import pandas as pd
import geopandas as gpd
import numpy as np
import itertools
from Levenshtein import distance

from utils.load_save import *
from utils.embedding import *
from utils.params import *
from utils.dictionary import *

import warnings
warnings.filterwarnings("ignore")

def find_geo_info(dataframe):
    lat = 'latitude'
    long = 'longitude'
    cur_crs = 'WGS84'

    geo_dataframe = gpd.GeoDataFrame(dataframe, geometry=gpd.points_from_xy(dataframe[long], dataframe[lat]))
    geo_dataframe.crs = cur_crs

    return dataframe

def compute_levenshtein(list_target, list_against):
    """
    compare_levenshtein function

    : input: list_target = 
    : input: list_against = 

    : return: dict_col_names = 
    """
    dict_col_names_lev = {'site_name': [],
                      'other_name': [],
                      'commodity': []}

    for c in list(itertools.product(list_target, list_against)):
        lev_distance = distance(c[1], c[0])

        if lev_distance < 3:
            dict_col_names_lev[c[0]].append(c[1])

    return dict_col_names_lev

def compute_cosine(against_dict):
    # TODO: make function that will compute cosine similarity of bert embedding on the definition of the column title
    # TODO: convert sentence and sentences in dictionary to embeddings
    dict_target = build_target_dic()


    return 0

def find_cols(filename):
    """
    find_cols function

    : input: filename = 

    : return: dict_col_name =
    : return: list_col_commodities = 
    """
    dataframe = pickle_load(DIR_TO_PKL, filename)
    list_against = dataframe.columns.str.lower()
    dataframe.columns = list_against

    find_cols(list_against)

    list_against = dataframe

    # TODO: add part to grab dictionary file given filename
    col_target = ['site_name', 'other_name', 'commodity']
    col_against = list_against    # TODO: change so that this is based on the dictionary file

    # Starting point initiation
    dict_col_names_lev = compute_levenshtein(col_target, col_against)

    col_org_sitename = dict_col_names_lev['site_name']
    col_org_othername = dict_col_names_lev['other_name']
    col_org_commodities = dict_col_names_lev['commodity']

    for c in [col_org_sitename, col_org_othername, col_org_commodities]:
        if len(c) == 0:
            print(c)

    return col_org_sitename, col_org_othername, col_org_commodities