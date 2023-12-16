from itertools import product
import umap.umap_ as umap
from sklearn.decomposition import PCA
import hdbscan
from statistics import *
from minelink.m3_text_based_linking.text_clustering import *

import pandas as pd
import polars as pl

def tsne_reduction_pl(pl_data):
    return 0

def umap_reduction_pl(pl_data):
    n_neighbors = [2, 5, 10, 15, 20, 30, 50]
    min_dist = [0, 0.2, 0.4, 0.6, 0.8, 1]
    n_components = [2, 3, 4, 5]
    metrics = ['cosine']

    combinations = list(product(n_neighbors, min_dist, n_components, metrics))

    pca = PCA(0.75).fit(pl_data['embeddings'].to_list())
    embeddings_pca_transformed = pca.transform(pl_data['embeddings'].to_list())

    pl_data = pl_data.with_columns(
        pca_embeddings = embeddings_pca_transformed
    )

    return 0

def tsne_reduction(df_data):
    return 0

def umap_reduction(df_data):
    """
    consist of index column, geometry column, document column, embeddding column. only get the embedding column
    """
    n_neighbors = [2, 5, 10, 15, 20, 30, 50]
    min_dist = [0, 0.2, 0.4, 0.6, 0.8, 1]
    n_components = [2, 3, 4, 5]
    metrics = ['cosine']

    combinations = list(product(n_neighbors, min_dist, n_components, metrics))

    # pca = PCA(0.75).fit(df_data['embeddings'].to_list())
    # embeddings_pca_transformed = pca.transform(df_data['embeddings'].to_list())

    # df_data = pd.concat([df_data, embeddings_pca_transformed], axis=1)

    for c in combinations:
        reducer = umap.UMAP(random_state= 0, n_neighbors = c[0], min_dist=c[1], n_components = c[2], metric='cosine', verbose = 0)
        umap_embeddings = reducer.fit_transform(df_data.embeddings.to_list())

        df_data['umap_embeddings'] = ""

        for index in range(len(df_data)):
            df_data['umap_embeddings'][index] = umap_embeddings[index]

        hdbscan_text(df_data)

    return df_data

def text_embedding_reduction(df_data):
    # Need to groupby GroupID before apply reduction
    df_data = df_data.groupby(by='GroupID').agg(lambda x: [x])
    df_data_potentials = umap_reduction(df_data)

    return df_data_potentials