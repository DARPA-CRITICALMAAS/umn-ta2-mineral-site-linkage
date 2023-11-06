import pandas as pd
import geopandas as gpd
import numpy as np

from utils.load_save import *
from utils.embedding import *

def select_string(item):
    if (isinstance(item, str)):
        return True
    return False

def create_text_emb(names, commodities, location):
    names = list(filter(None, str(names)))
    commodities = list(filter(None, commodities))
    location = list(filter(None, location))

    names = list(filter(select_string, names))
    commodities = list(filter(select_string, commodities))
    location = list(filter(select_string, location))
    # sentence = names + ' is a ' + ', '.join(commodities) + ' mine in ' + ', '.join(location)
    sentence = ', '.join(names) + ' is a ' + ', '.join(commodities) + ' mine'
    # sentence = ', '.join(names) + ' is a ' + ', '.join(commodities) + ' mine'
    # sentence = ', '.join(names)

    tokenizer, model = model_load('bert-base-uncased')
    emb_sentence = text_embedding(tokenizer=tokenizer, model=model, sentence=sentence)

    return emb_sentence

def convert_to_document(dataframe, col_available_loc):
    """
    'convert_to_document' function

    : input: dataframe
    : input: col_available_loc:

    : return: dataframe = original dataframe with addtional columns of text embedding and location embedding
    """
    dataframe['text'] = dataframe.apply(lambda x: create_text_emb(x['site_name'], x['commodities'], x[col_available_loc]), axis=1) #TODO: if country or county exists those needs to be added too
    # dataframe = princip_comp_analysis(dataframe)

    dataframe['location'] = dataframe['geometry']   #.apply(lambda l: spatial_embedding(l.x, l.y))
    # df_emb = pds.concat([dataframe['idx'], df_text, df_loc], axis=1)
    # df_emb.columns = ['idx', 'text', 'location']
    # df_emb = pd.concat([dataframe['idx'], df_text], axis=1)

    pickle_dump(dataframe, './', 'MRDS')

    return dataframe