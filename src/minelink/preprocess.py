import os

import pickle5 as pickle
from json import loads, dumps

import pandas as pd
import geopandas as gpd
from shapely import wkt
# from transformers import AutoTokenizer, AutoModel

from params import *

def loader(path_dir, file_name, extension):
    if extension == '.gdb':
        # return gpd.read_file(os.path.join(path_dir, file_name + extension), driver="OpenFileGDB") 
        return 'gdb', file_name
    elif extension == '.pkl':
        with open(os.path.join(path_dir, file_name+'.pkl'), 'rb') as handle:
            dataframe = pickle.load(handle)
        # return dataframe
        return 'pkl', file_name
    elif extension == '.csv':
        # return(pd.read_csv(os.path.join(path_dir, file_name + extension)))
        return 'csv', file_name
    else:
        print('[' + file_name + extension + ']' + " is not a file with acceptable input type")
        return -1

def dumper(file, extension):
    return 0

def open_dir(path_dir):
    list_dir_items = os.listdir(path_dir)
    acceptable_items = ['',]

    for l in list_dir_items:
        file_name, file_extension = os.path.splitext(l)
        if file_extension == '':
            pass
        else: 
            print(loader(path_dir, file_name, file_extension))

    print("Model running with the following files: " + "\n\t".join(acceptable_items))
    
    bool_confirm = False
    while not bool_confirm:
        user_confirm = str(input("> Confirmed (Y/N)?:")).upper()
        if user_confirm == 'Y':
            bool_confirm = True

open_dir('/home/yaoyi/pyo00005/CriticalMAAS/src/data')