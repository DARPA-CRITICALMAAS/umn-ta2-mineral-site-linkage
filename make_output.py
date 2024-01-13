import pandas as pd
import pickle
import json
import polars as pl
from itertools import product
from sklearn.cluster import HDBSCAN
import numpy as np

from sklearn.metrics.cluster import completeness_score
from sentence_transformers import SentenceTransformer
import umap.umap_ as umap

# Preprocessing
file_MRDS = '/home/yaoyi/pyo00005/CriticalMAAS/src/data/raw/MRDS.csv'
file_USMIN = '/home/yaoyi/pyo00005/CriticalMAAS/src/data/raw/USMIN_IDMT.csv'

dict_file_MRDS = '/home/yaoyi/pyo00005/CriticalMAAS/src/data/raw/dict_MRDS.csv'
dict_file_USMIN = '/home/yaoyi/pyo00005/CriticalMAAS/src/data/raw/dict_USMIN.csv'

df_MRDS = pl.read_csv(file_MRDS)
df_MRDS = df_MRDS.with_columns(
    rep_id = 'MRDS_' + pl.col('OBJECTID').cast(pl.Utf8)
)

df_USMIN = pl.read_csv(file_USMIN, truncate_ragged_lines=True)
df_USMIN = df_USMIN.select(
    pl.col(['OBJECTID', 'Site_ID', 'Ftr_ID', 'Ftr_Name', 'Other_Name', 'Last_Updt', 'Ftr_Group', 'Ftr_Type', 'Commodity', 'Lat_WGS84', 'Long_WGS84', 'Pt_Def', 'Poly_Def', 'State', 'County', 'Loc_Scale', 'Loc_Date', 'Ref_Detail', 'Ref_ID', 'Remarks', 'Loc_Poly']),
    rep_id = 'USMIN_' + pl.col('OBJECTID').cast(pl.Utf8)
)

dict_MRDS = pl.read_csv(dict_file_MRDS)
dict_MRDS = dict(zip(dict_MRDS['attribute_label'], dict_MRDS['short_description']))

dict_USMIN = pl.read_csv(dict_file_USMIN)
dict_USMIN = dict_USMIN.select(
    label = pl.col('Field Name').str.strip_chars(),
    short = pl.col('Short Description').str.strip_chars()
)
dict_USMIN = dict(zip(dict_USMIN['label'], dict_USMIN['short']))

dict_MRDS_columns = list(dict_MRDS.keys())
dict_USMIN_columns = list(dict_USMIN.keys())

df_MRDS_columns = list(df_MRDS.columns)
df_USMIN_columns = list(df_USMIN.columns)

coexists_MRDS = list(set(dict_MRDS_columns) & set(df_MRDS_columns))
coexists_USMIN = list(set(dict_USMIN_columns) & set(df_USMIN_columns))

# Get Ground Truth
file_ground_truth = '/home/yaoyi/pyo00005/CriticalMAAS/src/data/TungstenSkarn/MineralSites.csv'
df_ground_truth = pl.read_csv(file_ground_truth)

df_ground_truth = df_ground_truth.with_columns(
    pl.col('source_ID').str.split('; ')
).explode('source_ID')

df_MRDS_ground_truth = df_ground_truth.filter(
    pl.col('source_ID').str.contains('MRDS')
).with_columns(
    sort_factor = pl.col('source_ID').str.split('_').list.last().cast(pl.Int64)
).sort('sort_factor').drop('sort_factor')

df_USMIN_ground_truth = df_ground_truth.filter(
    pl.col('source_ID').str.contains('USMIN')
).with_columns(
    sort_factor = pl.col('source_ID').str.split('_').list.last().cast(pl.Int64)
).sort('sort_factor').drop('sort_factor')

# Location based linking
coords_MRDS = df_MRDS[['Long_NAD83', 'Lat_NAD83']].to_numpy()
clusters_MRDS = HDBSCAN(min_cluster_size=2, cluster_selection_epsilon=0.05).fit(coords_MRDS)

df_loc_linked_MRDS = df_MRDS.select(
    sort_factor = pl.col('OBJECTID'),
    idx = pl.col('rep_id'),
    GroupID = pl.Series(clusters_MRDS.labels_),
    probability = pl.Series(clusters_MRDS.probabilities_)
)
count_total_MRDS = df_loc_linked_MRDS.shape[0]

df_loc_linked_MRDS_group = df_loc_linked_MRDS.filter(
    pl.col('GroupID') != -1
)
df_loc_linked_MRDS_indiv = df_loc_linked_MRDS.filter(
    pl.col('GroupID') == -1
).drop('GroupID')
count_indiv_MRDS = df_loc_linked_MRDS_indiv.shape[0]
indiv_groups_MRDS = np.arange(count_total_MRDS, count_total_MRDS+count_indiv_MRDS)
df_loc_linked_MRDS_indiv = df_loc_linked_MRDS_indiv.with_columns(
    GroupID = pl.Series(indiv_groups_MRDS)
).select(
    pl.col(['sort_factor', 'idx', 'GroupID', 'probability'])
)
df_loc_linked_MRDS = pl.concat(
    [df_loc_linked_MRDS_group, df_loc_linked_MRDS_indiv]
).sort('sort_factor')

coords_USMIN = df_USMIN[['Long_WGS84', 'Lat_WGS84']].to_numpy()
clusters_USMIN = HDBSCAN(min_cluster_size=2).fit(coords_USMIN)

df_loc_linked_USMIN = df_USMIN.select(
    sort_factor = pl.col('OBJECTID'),
    idx = pl.col('rep_id'),
    GroupID = pl.Series(clusters_USMIN.labels_),
    probability = pl.Series(clusters_USMIN.probabilities_)
)
count_total_USMIN = df_loc_linked_USMIN.shape[0]

df_loc_linked_USMIN_group = df_loc_linked_USMIN.filter(
    pl.col('GroupID') != -1
)
df_loc_linked_USMIN_indiv = df_loc_linked_USMIN.filter(
    pl.col('GroupID') == -1
).drop('GroupID')
count_indiv_USMIN = df_loc_linked_USMIN_indiv.shape[0]
indiv_groups_USMIN = np.arange(count_total_USMIN, count_total_USMIN+count_indiv_USMIN)
df_loc_linked_USMIN_indiv = df_loc_linked_USMIN_indiv.with_columns(
    GroupID = pl.Series(indiv_groups_USMIN)
).select(
    pl.col(['sort_factor', 'idx', 'GroupID', 'probability'])
)
df_loc_linked_USMIN = pl.concat(
    [df_loc_linked_USMIN_group, df_loc_linked_USMIN_indiv]
).sort('sort_factor')

df_MRDS = df_MRDS.drop('rep_id').with_columns(
    pl.when(pl.all().is_null())
    .then(pl.lit(' '))
    .otherwise(pl.all())
    .name.keep()
).drop(['OBJECTID', 'dep_id', 'country', 'state', 'latitude', 'longitude', 'Lat_NAD83', 'Long_NAD83'])

df_USMIN = df_USMIN.drop('rep_id').with_columns(
    pl.when(pl.all().is_null())
    .then(pl.lit(' '))
    .otherwise(pl.all())
    .name.keep()
).drop(['OBJECTID', 'Site_ID', 'Lat_WGS84', 'Long_WGS84'])

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

def global_embedding_reduction(pl_data):
    list_embeddings = pl_data['embeddings'].to_list()

    n_neighbors = [5, 10, 15, 20, 30, 50, 100, 200]
    min_dist = [0, 0.2, 0.4, 0.6, 0.8, 1]
    n_components = [2, 4, 8, 16, 32, 64, 128]

    combinations = list(product(n_neighbors, min_dist, n_components))

    list_umap_embeddings = []

    for c in combinations:
        reducer = umap.UMAP(n_neighbors=c[0], min_dist=c[1], n_components=c[2], metric='cosine', verbose = 0)
        umap_embeddings = reducer.fit_transform(list_embeddings)
        list_umap_embeddings.append(umap_embeddings.tolist())

    return list_umap_embeddings

n_neighbors = [5, 10, 15, 20, 30, 50, 100, 200]
min_dist = [0, 0.2, 0.4, 0.6, 0.8, 1]
n_components = [2, 4, 8, 16, 32, 64, 128]

combinations = list(product(n_neighbors, min_dist, n_components))