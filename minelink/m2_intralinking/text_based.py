import polars as pl
import umap.umap_ as umap
import hdbscan
from itertools import product

from minelink.params import *
from minelink.m0_load_input.load_data import load_file

from sentence_transformers import SentenceTransformer

# def embedding_reduction(txt_embed):

def global_embedding_reduction(pl_data):
    list_embeddings = pl_data['embeddings'].to_list()

    n_neighbors = [5, 10, 15, 20, 30, 50]
    min_dist = [0, 0.2, 0.4, 0.6, 0.8, 1]
    n_components = [2, 3, 4, 5]

    combinations = list(product(n_neighbors, min_dist, n_components))

    dict_umap_embeddings = {}
    list_umap_embeddings = []

    for c in combinations:
        reducer = umap.UMAP(n_neighbors=c[0], min_dist=c[1], n_components=c[2], metric='cosine', verbose = 0)
        umap_embeddings = reducer.fit_transform(list_embeddings)
        # dict_umap_embeddings[str(c)] = umap_embeddings
        list_umap_embeddings.append(umap_embeddings)

    return list_umap_embeddings

# def local_embedding_reduction(pl_data):


def link_embeddings():
    clsuter = hdbscan.HDBSCAN(min_samples=1)
    # cluster.fit(df_data['reduced_embedding'])

def embedding_reduction(list_grouped_embedding):
    # list_embeddings = pl_data['embeddings'].to_list()
    list_embeddings = list_grouped_embedding['embeddings']

    if len(list_embeddings) == 0:
        return list_embeddings

    reducer = umap.UMAP(n_neighbors = 2, min_dist=0.2, n_components = 3, metric='cosine', verbose = 0)
    umap_embeddings = reducer.fit_transform(list_embeddings)

    return umap_embeddings

# def hdbscan_text(df_data):
#     cluster = hdbscan.HDBSCAN(min_samples=1)
#     cluster.fit(df_data['reduced_embedding'].to_list())

#     df_data['labels'] = cluster.labels_
#     df_data['probabilities'] = cluster.probabilities_

#     # df_data = df_data.loc[df_data['labels'] != -1]              # means individual cluster
#     df_data = df_data.loc[df_data['probabilities'] > 0.8]       # only those with high confidence

#     return df_data

def create_document(struct_data, dictionary):
    data_title = list(struct_data[0].keys())
    data_value = list(struct_data[0].values())

    document = ''

    for i in range(len(data_title)):
        if data_value[i] == ' ':
            continue

        try:
            word = dictionary[data_title[i]]
            value = data_value[i]
            document = document + word + " is " + value + ". "
        except:
            pass

    model = SentenceTransformer('all-mpnet-base-v2')
    txt_embed = model.encode(document)
    txt_embed = txt_embed.flatten().tolist()

    return (txt_embed, )

def text_based_linking(alias_code, pl_loc_linked):
    pl_tolink = load_file([PATH_TMP_DIR, alias_code],
                          'df_tolink',
                          '.pkl')
    dictionary = load_file([PATH_TMP_DIR, alias_code],
                           'mini_dictionary',
                           '.pkl')
    
    pl_tolink = pl_tolink.sort('idx')

    # pl_idx = pl_tolink.select(
    #     pl.col('idx')
    # )
    pl_tolink = pl_tolink.drop('idx').with_columns(
        pl.when(pl.all() == None)
        .then(pl.lit(' '))
        .otherwise(pl.all())
        .name.keep()
    )

    # TODO: remove later
    # pl_idx = pl_idx.head(4)
    # pl_tolink = pl_tolink.head(4)
    # pl_loc_linked = pl_loc_linked.head(4)

    pl_doc = pl_tolink.select(
        pl.struct(pl.all()).alias('data_as_struct')
    ).map_rows(
        lambda x: (create_document(x, dictionary))
    ).rename({'column_0':'embeddings'})

    pl_doc = pl.concat(
        [pl_doc, pl_loc_linked], 
        how='horizontal'
    )
    
    # TODO: Test on local level embedding reduction
    # pl_text_linked = pl_doc.group_by(
    #     'GroupID'
    # ).agg(
    #     [pl.all()]
    # ).with_columns(
    #     pl.struct('embeddings').apply(embedding_reduction).alias('reduced')
    # )

    # print(pl_text_linked)

    reduced_embedding = global_embedding_reduction(pl_doc)

    pl_text_linked = pl_doc.with_columns(
        reduced = pl.Series(reduced_embedding)
    )

    pl_text_linked = pl_text_linked.select(
        pl.col(['idx', 'GroupID'])
    )

    return pl_doc, pl_text_linked