import numpy as np
import pandas as pd
import geopandas as gpd
from transformers import AutoTokenizer, AutoModel
from sentence_transformers import SentenceTransformer

from minelink.m0_save_and_load.load_data import *
from minelink.m0_save_and_load.save_ckpt_as_pickle import save_ckpt

xfrmer_model = 'bert-base-uncased'
snt_xfrmer = 'all-distilroberta-v1'

def add_is(columns, df_cells, dictionary):
    if df_cells:
        if df_cells[0] == ' ':
            return ''
        if columns.lower() in dictionary.keys():
            return dictionary[columns.lower()] + 'is ' + ', '.join(str(i) for i in df_cells) + '. '
        
    return ''

def text_to_embedding(input_text):
    model = AutoModel(snt_xfrmer)
    txt_embed = model.encode(input_text, convert_to_tensor=True)

    return txt_embed

def create_documents(df_tolink, path_stored):
    """
    : input: df_tolink (gpd)
    : input: source_lias_code (str)
    """
    # load df_dict based on source alias code
    df_dictionary = load_file(path_stored, 'dictionary', '.pkl')

    dictionary = dict(zip(df_dictionary['label'], df_dictionary['short']))

    df_documentized = gpd.GeoDataFrame()
    df_documentized['unique_id'] = df_tolink.index.astype(str)
    df_documentized['geometry'] = df_tolink['geometry']
    df_documentized['document'] = pd.DataFrame(np.vectorize(add_is)(df_tolink.columns, df_tolink, dictionary)).sum(axis=1)
    df_documentized['embedding'] = df_documentized['document'].apply(lambda x: text_to_embedding(x))

    save_ckpt(df_documentized, path_stored, 'df_doc')

    return df_documentized
