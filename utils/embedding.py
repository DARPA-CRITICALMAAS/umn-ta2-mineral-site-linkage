import torch
import math
import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE

from utils.params import *

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

def space2vec(list_coord):
    """
    space2vec function uses the positional encoder presented in 'Multi-Scale Representation Learning for Spatial Feature Distributions using Grid Cells'
    It transforms the input coordinates (lat, long) to embeddings by using sinusoidal functions and re-projection to desired output sapce through linear layers

    : input: lat =
    : input: long = 

    : return: 

    From: https://github.com/gengchenmai/space2vec/tree/master and https://github.com/konstantinklemmer/pe-gnn/tree/main
    """
    num_freq = 2
    radius_max = 10000
    radius_min = 10
    
    log_timescale_increment = (math.log(float(radius_max) / float(radius_min)) / (num_freq*1.0 - 1))
    timescale = radius_min * np.exp(np.arange(num_freq).astype(float) * log_timescale_increment)
    list_freq = 1.0 / timescale
    matrix_freq = np.expand_dims(list_freq, axis=1)
    matrix_freq = np.repeat(matrix_freq, 2, axis=1)
    
    # Shape: (1, 2)
    matrix_coord = np.asmatrix(np.asarray(list_coord).astype(float))
    num_context_pt = matrix_coord.shape[1]  # Should be 2

    # Shape: (1, 2, 1)
    matrix_coord = np.expand_dims(matrix_coord, axis=2)
    # Shape: (1, 2, 1, 1)
    matrix_coord = np.expand_dims(matrix_coord, axis=3)
    # Shape: (1, 2, 384, 1)
    matrix_coord = np.repeat(matrix_coord, num_freq, axis=2)

    spr_embeds = matrix_coord * matrix_freq

    spr_embeds[:, :, :, 0::2] = np.sin(spr_embeds[:, :, :, 0::2])  # dim 2i
    spr_embeds[:, :, :, 1::2] = np.cos(spr_embeds[:, :, :, 1::2])  # dim 2i+1

    spr_embeds = spr_embeds.flatten()
    # print(spr_embeds.shape)

    return spr_embeds

def spatial_embedding(lat, long):
    list_coord = [lat, long]
    
    return space2vec(list_coord)

def princip_comp_analysis(dataframe):
    """
    'princip_comp_analysis' function performs principle componenet analysis for the purpose of dimension reduction on text embeddings that will be used to cluster

    : input: dataframe = 
    : return: dataframe = dataframe with reduced dimensionality on the text embeddings
    """
    col_text = dataframe['text']

    arr_emb = np.array(col_text.values.tolist())
    # arr_emb = np.array(list_emb)
    arr_emb = np.transpose(arr_emb)

    # Component size of PCA (just to satisfy the requirements of PCA)
    dim_emb = PCA_COMPONENTS if PCA_COMPONENTS < min(arr_emb.shape[0], arr_emb.shape[1]) else min(arr_emb.shape[0], arr_emb.shape[1])

    pca = PCA(n_components=dim_emb)
    reduced_embedding = pca.fit(arr_emb)
    # pca_comp = reduced_embedding
    pca_comp = np.asarray(pca.components_)
    pca_comp = np.transpose(pca_comp)

    series_embedding = pd.Series(pca_comp.tolist(), name='reduct_text')

    dataframe = pd.concat([dataframe, series_embedding], axis=1)

    return dataframe

def tsne(dataframe):
    col_text = dataframe['text']

    arr_emb = np.array(col_text.values.tolist())
    # arr_emb = np.array(list_emb)
    # arr_emb = np.transpose(arr_emb)

    # print(arr_emb.shape)

    perplexity_val = (dataframe.shape[0] - 1)
    # print(perplexity_val)

    reduced_embedding = TSNE(
        n_components=3,
        learning_rate='auto',
        init='pca',
        perplexity=perplexity_val
    ).fit_transform(arr_emb)
    # tsne_comp = np.asarray(reduced_embedding.components_)
    # tsne_comp = np.transpose(tsne_comp)

    # print(reduced_embedding.shape)

    series_embedding = pd.Series(reduced_embedding.tolist(), name='reduct_text')
    dataframe = dataframe.reset_index(drop=True)

    dataframe = pd.concat([dataframe, series_embedding], axis=1)

    return dataframe
    

def text_embedding(tokenizer, model, sentence):
    layers = [-4, -3, -2, -1]
    encoded_sentence = tokenizer.encode_plus(sentence, return_tensors="pt")

    with torch.no_grad():
        output = model(**encoded_sentence)

    states = output.hidden_states
    token_output = torch.stack([states[i] for i in layers]).sum(0).squeeze()
    sent_embedding = token_output.mean(dim=0)
    sent_embedding = sent_embedding.numpy()

    return sent_embedding

def geolm_embedding():
    return 0 # Will happen later