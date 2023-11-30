import yake
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

from sentence_transformers import SentenceTransformer, util
from minelink.m0_save_and_load.save_load_file import load_file

xfrmer_model = 'all-MiniLM-L6-v2'

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

def df_to_dictionary(df):
    dict_description = df
    
    return dict_description