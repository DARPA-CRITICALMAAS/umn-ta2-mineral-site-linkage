import regex as re
import polars as pl
import pandas as pd

# from utils.compare_text import *
# file_name = '/home/yaoyi/pyo00005/CriticalMAAS/src/MINMOD_DATA/https:pubs.usgs.govof20081155/pubs.usgs.gov/of/2008/1155/dictionary.csv'
# file_name = '/home/yaoyi/pyo00005/CriticalMAAS/src/MINMOD_DATA/usmin/USGS_Gallium_US_CSV/Data_Dictionary.csv'
file_name = '/home/yaoyi/pyo00005/CriticalMAAS/src/MINMOD_DATA/MRDS/dictionary.csv'

data_dictionary = pl.read_csv(file_name)

print(data_dictionary)