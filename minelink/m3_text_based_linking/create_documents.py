import numpy as np
import pandas as pd
import geopandas as gpd

from minelink.m0_save_and_load.load_data import *
from minelink.m0_save_and_load.save_ckpt_as_pickle import save_ckpt

def add_is(columns, df_cells, dictionary):
    if df_cells:
        if df_cells[0] == ' ':
            return ''
        if columns.lower() in dictionary.keys():
            return dictionary[columns.lower()] + 'is ' + ', '.join(str(i) for i in df_cells) + '. '
        
    return ''

def create_documents(df_tolink, source_alias_code):
    """
    : input: df_tolink (gpd)
    : input: source_lias_code (str)
    """
    # load df_dict based on source alias code
    df_dictionary = load_file(PATH_TMP_DIR, 'dictionary', '.pkl', additional=source_alias_code)
    dictionary = dict(zip(df_dictionary['label'], df_dictionary['short']))

    df_documentized = gpd.GeoDataFrame()
    df_documentized['unique_id'] = df_tolink.index.astype(str)
    df_documentized['geometry'] = df_tolink['geometry']
    df_documentized['document'] = pd.DataFrame(np.vectorize(add_is)(df_tolink.columns, df_tolink, dictionary)).sum(axis=1)

    save_ckpt(df_documentized, PATH_TMP_DIR, 'df_doc', additional=source_alias_code)

    df_documentized = df_tolink

    return df_documentized