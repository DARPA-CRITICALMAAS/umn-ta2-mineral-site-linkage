#!/usr/bin/python

import os
import shutil

import regex as re
import pandas as pd
import geopandas as gpd
import pickle5 as pickle

from minelink.params import *

def check_dir(path_dir, additional=''):
    """
    check_dir function checkes whether the inputted directory path exists and creates one if it does not exist
    
    input: path_dir=path to directory
    """
    path_dir = os.path.join(path_dir, additional)

    if not os.path.exists(path_dir):
        os.makedirs(path_dir)

def load_file(path_dir, file_name, extension, additional=''):
    path_dir = os.path.join(path_dir, additional)
    
    if extension == '.gdb':
        return gpd.read_file(os.path.join(path_dir, file_name + extension), driver="OpenFileGDB")
    
    elif extension == '.geojson':
        return gpd.read_file(os.path.join(path_dir, file_name + extension))
    
    elif extension == '.pkl':
        with open(os.path.join(path_dir, file_name+'.pkl'), 'rb') as handle:
            dataframe = pickle.load(handle)
        return dataframe
    
    elif extension == '.csv':
        return(pd.read_csv(os.path.join(path_dir, file_name + extension)))
    
    elif extension == '.xls' or extension == '.xlsx':
        return(pd.read_excel(os.path.join(path_dir, file_name + extension)))
    
    # TODO: Somethng is wrong here; it needs to be editted so that later user inputs can also be read
    # elif extension == '.json':
    #     f = open(os.path.join(path_dir, file_name + extension), 'r')
    #     data  = loads(f)
    #     return data

def open_dir(path_dir, bool_dict=False):
    list_dir_items = os.listdir(path_dir)

    list_files = []
    list_dict = []

    if bool_dict==False:
        return list_dir_items, len(list_dir_items)

    for l in list_dir_items:
        file_name, file_extension = os.path.splitext(l)

        if file_extension != '':
            if re.search('dict', file_name):
                list_dict.append(l)
            else:
                list_files.append(l)
        else:
            list_dict_items = os.listdir(os.path.join(path_dir, l))
            list_dict_items = [l+'/'+i for i in list_dict_items]
            list_dict.extend(list_dict_items)

    return list_files, len(list_files), list_dict

def move_file(path_org_file, path_mv_dir):
    try: 
        shutil.copy(path_org_file, path_mv_dir)
        return 0
    except:
        return -1

