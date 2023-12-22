import numpy as np
import polars as pl
import umap.umap_ as umap
import hdbscan
from itertools import product

from minelink.params import *
from minelink.m0_load_input.load_data import load_file

from sentence_transformers import SentenceTransformer

def global_embedding_reduction(pl_data):
    list_embeddings = pl_data['embeddings'].to_list()

    n_neighbors = [5, 10, 15, 20, 30, 50]
    min_dist = [0, 0.2, 0.4, 0.6, 0.8, 1]
    n_components = [16, 32, 128]

    combinations = list(product(n_neighbors, min_dist, n_components))

    list_umap_embeddings = []

    for c in combinations:
        reducer = umap.UMAP(n_neighbors=c[0], min_dist=c[1], n_components=c[2], metric='cosine', verbose = 0)
        umap_embeddings = reducer.fit_transform(list_embeddings)
        list_umap_embeddings.append(umap_embeddings.tolist())

    return list_umap_embeddings

# def local_embedding_reduction(counter, group_id, list_embeddings):
#     if group_id == -1:
#         return -1, 0
    
#     n_neighbors = list(range(2, len(list_embeddings)))
#     min_dist = [0, 0.2, 0.4, 0.6, 0.8, 1]
#     n_components = list(range(1, len(list_embeddings)))

#     combinations = list(product(n_neighbors, min_dist, n_components))

#     reducer = umap.UMAP(n_neighbors=len(list_embeddings)-1, min_dist=0.2, n_components=len(list_embeddings)-1, metric='cosine', verbose = 0)
#     umap_embeddings = reducer.fit_transform(list_embeddings)

#     # for c in combinations:
#     #     reducer = umap.UMAP(n_neighbors=c[0], min_dist=c[1], n_components=c[2], metric='cosine', verbose = 0)
#     #     umap_embeddings = reducer.fit_transform(list_embeddings)

#     #     print(umap_embeddings)

#     return group_id, len(list_embeddings)

def hdbscan_text(list_embeddings):
    cluster = hdbscan.HDBSCAN(min_samples=2)
    cluster.fit(list_embeddings)

    return cluster.labels_, np.sum(cluster.probabilities_)

def text_clustering(pl_row, total_count):
    GroupID = pl_row[0]
    loc_grouped_idx = pl_row[1]
    num_items = len(loc_grouped_idx)

    if GroupID == -1:
        return np.repeat(-1, num_items).tolist(), loc_grouped_idx
     
    max_labels = []
    max_prob = 0

    for i in range(2, len(pl_row)):
        labels, probabilities = hdbscan_text(pl_row[i])
        if probabilities > max_prob:
            max_labels = labels

    if max_prob == 0:
        return np.repeat(-1, num_items).tolist(), loc_grouped_idx
	
    unique_groups = np.unique(max_labels)
    group_alias = list(np.arange(0, unique_groups.size) * total_count + GroupID)
    group_map = dict(zip(list(unique_groups), group_alias))

    new_grouping = list(map(lambda x: group_map[x], max_labels))

    return new_grouping, loc_grouped_idx

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

    model = SentenceTransformer('all-mpnet-base-v2', device='cuda')
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

    pl_tolink = pl_tolink.drop('idx').with_columns(
        pl.when(pl.all() == None)
        .then(pl.lit(' '))
        .otherwise(pl.all())
        .name.keep()
    )

    # TODO: ACTIVATE LATER
    pl_doc = pl_tolink.select(
        pl.struct(pl.all()).alias('data_as_struct')
    ).map_rows(
        lambda x: (create_document(x, dictionary))
    ).rename({'column_0':'embeddings'})

    # pl_doc = pl.concat(
    #     [pl_loc_linked, pl_doc], 
    #     how='horizontal'
    # )

    reduced_embedding = global_embedding_reduction(pl_doc)
    pl_reduced_embeddings = pl.DataFrame(
        reduced_embedding
    )
    # .transpose(
    #     include_header=False
    # )

    pl_reduced_embeddings = pl.concat(
        [pl_loc_linked, pl_reduced_embeddings],
        how='horizontal'
    )

    pl_text_linked = pl_reduced_embeddings.filter(
         pl.col('GroupID') != None
    ).group_by(
        'GroupID'
    ).agg(
        [pl.all()]
    )

    total_count = pl_text_linked.shape[0]

    pl_text_linked = pl_text_linked.map_rows(
        lambda x: text_clustering(x, total_count)
    ).explode(
        'column_0', 'column_1'
    ).rename(
        {'column_0':'GroupID', 'column_1':'idx'}
    )

    return pl_doc, pl_reduced_embeddings, pl_text_linked