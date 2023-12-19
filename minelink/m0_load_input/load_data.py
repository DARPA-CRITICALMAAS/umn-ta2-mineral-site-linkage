#!/usr/bin/python

import os
import shutil

import regex as re
import polars as pl
import geopandas as gpd
import pickle5 as pickle

from minelink.params import *
from minelink.m0_load_input.save_ckpt import *

def load_file(list_path, file_name, file_extension):
    file_path = ''
    for i in list_path:
        file_path = os.path.join(file_path, i)

    if file_extension == '.gdb':
        return gpd.read_file(os.path.join(file_path, file_name + file_extension), driver="OpenFileGDB")
    
    elif file_extension == '.geojson':
        return 0
    
    elif file_extension == '.pkl':
        with open(os.path.join(file_path, file_name+'.pkl'), 'rb') as handle:
            dataframe = pickle.load(handle)
        return dataframe

    elif file_extension == '.csv':
        dataframe = pl.read_csv(os.path.join(file_path, file_name+'.csv'))
        return dataframe
    
    elif file_extension == '.xls' or file_extension == 'xlsx':
        dataframe = pl.read_excel(
            source=os.path.join(file_path, file_name + file_extension),
            engine='openpyxl',
        )
        return dataframe
    
    elif file_extension == '.json':
        return (pl.read_json(os.path.join(file_path, file_name + file_extension)))
    
    else:
        print("INVALID INPUT TYPE")
        return -1

def move_file(list_path_org, list_path_mv_dir):
    path_org_file = ''
    for i in list_path_org:
        path_org_file = os.path.join(path_org_file, i)

    path_mv_dir = ''
    for i in list_path_mv_dir:
        path_mv_dir = os.path.join(path_mv_dir, i)

    try: 
        shutil.copy(path_org_file, path_mv_dir)
        return 0
    except:
        return -1

def read_dir(path_dir, bool_dict):
    list_dir_items = os.listdir(path_dir)

    list_files = []
    list_dict = []

    if bool_dict==False:
        return list_dir_items, len(list_dir_items)
    
    for l in list_dir_items:
        file_name, file_extension = os.path.splitext(l)

        if file_extension == '':
            list_dict_items = os.listdir(os.path.join(path_dir, l))
            list_dict_items = [l+'/'+i for i in list_dict_items]
            list_dict.extend(list_dict_items)
        else:
            if re.search('dict', file_name):
                list_dict.append(l)
            else:
                list_files.append(l)

    return list_files, list_dict

def load_dir(path_dir, bool_dict=False):
    list_files, list_dict = read_dir(path_dir, bool_dict)

    open_ckpt_dir([PATH_TMP_DIR, 'dictionary'])
    list_dict_names = []

    for d in list_dict:
        file_name, file_extension = os.path.splitext(d)

        try:
            raw_file_name = re.split('/', file_name)[1]
        except:
            raw_file_name = file_name

        if re.search('dict_', raw_file_name):
            raw_file_name = re.split('dict_', raw_file_name)[1]

        list_dict_names.append(raw_file_name)

        pl_dictionary = load_file([path_dir], file_name, file_extension)
        save_ckpt(pl_dictionary, [PATH_TMP_DIR, 'dictionary'], raw_file_name)

    dict_code_alias = {}
    leading_char = 'a'
    follow_char = 'c'

    for f in list_files:
        file_name, file_extension = os.path.splitext(f)

        df = load_file([path_dir], file_name, file_extension)

        alias_code = leading_char + follow_char
        open_ckpt_dir(list_path=[PATH_TMP_DIR, alias_code])

        try:
            source_id = re.sub('_', '/', file_name)
        except:
            source_id = file_name

        dict_code_alias[alias_code] = source_id
        save_ckpt(df, [PATH_TMP_DIR, alias_code], 'raw')

        if file_name in list_dict_names:
            move_file([PATH_TMP_DIR, 'dictionary', file_name+'.pkl'], [PATH_TMP_DIR, alias_code, 'dictionary.pkl'])

        follow_char = chr(ord(follow_char) + 1) 
        leading_char = chr(ord(leading_char) + 1) if follow_char == 'a' else leading_char

    save_ckpt(dict_code_alias, [PATH_TMP_DIR], 'alias_code')

    return list(dict_code_alias.keys())