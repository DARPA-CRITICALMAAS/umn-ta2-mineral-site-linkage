import pickle5 as pickle
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from sentence_transformers import SentenceTransformer, util
import os
import pandas as pd

from utils.load_save import *
from utils.params import *

def build_target_dic():
    """
    'build_target_dic' function

    : return: dict_target
    """
    dict_target = {'site_name': 'Name of mineral site',
                   'other_name': 'Other names of mineral site',
                   'commodity': 'Commodities at mineral site',}

    return dict_target

def build_against_dic(filename):
    df_against = csv_load(DIR_TO_DICT, filename)
    df_against = df_against[['Field Name', 'Description']]

    list_against = df_against['Field Name'].tolist()

    df_against.to_csv('tmp.csv')

    return list_against, df_against

def convert_to_against_dict(dataframe):

    dict_against = dataframe.to_dict('index')

    return dict_against