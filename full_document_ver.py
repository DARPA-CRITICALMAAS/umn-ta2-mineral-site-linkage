import pickle
import polars as pl

from sklearn.metrics.pairwise import cosine_similarity
from make_output import *

path_dir = '/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/document_testing'

# PART 3
with open('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/document_testing/full_MRDS.pkl', 'rb') as handle:
    full_MRDS = pickle.load(handle)

df_MRDS_gt_grp = df_MRDS_ground_truth.select(
    pl.col('OID_')
)

pl_appended_MRDS = pl.concat(
    [full_MRDS, df_loc_linked_MRDS, df_MRDS_gt_grp],
    how='horizontal'
).rename(
    {'OID_':'ground_truth'}
)

pl_MRDS_with_group = pl_appended_MRDS.group_by(
    'ground_truth'
).agg(
    [pl.all()]
).with_columns(
    count = pl.col('idx').list.len()
).filter(
    pl.col('count') > 1
).explode(
    'embeddings', 'sort_factor', 'idx', 'GroupID'
).drop(
    'probability', 'count'
)

# pl_MRDS_with_group = pl_appended_MRDS.group_by(
#     'ground_truth'
# ).agg(
#     [pl.all()]
# ).with_columns(
#     count = pl.col('idx').list.len()
# ).explode(
#     'embeddings', 'sort_factor', 'idx', 'GroupID'
# ).drop(
#     'probability'
# )

def textual_cosine_similarity(pl_data):
    pl_data_partitioned = pl_data.partition_by(
        'GroupID'
    )

    for i in pl_data_partitioned:
        embeddings_list = i['embeddings'].to_list()

        print(cosine_similarity(embeddings_list, embeddings_list))

    print(pl_data_partitioned)

    return 0

textual_cosine_similarity(pl_MRDS_with_group)

with open('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/document_testing/full_MRDS_reduced.pkl', 'rb') as handle:
    full_MRDS_reduced = pickle.load(handle)

full_MRDS_reduced = full_MRDS_reduced.select(
    column_0 = pl.col('column_2')
)

pl_appended_MRDS_reduced = pl.concat(
    [full_MRDS, full_MRDS_reduced, df_loc_linked_MRDS, df_MRDS_gt_grp],
    how='horizontal'
).drop(
    'embeddings'
).rename(
    {'OID_':'ground_truth', 'column_0':'embeddings'}
)

pl_MRDS_with_group = pl_appended_MRDS_reduced.group_by(
    'ground_truth'
).agg(
    [pl.all()]
).with_columns(
    count = pl.col('idx').list.len()
).filter(
    pl.col('count') > 1
).explode(
    'embeddings', 'sort_factor', 'idx', 'GroupID'
).drop(
    'probability', 'count'
)

# textual_cosine_similarity(pl_MRDS_with_group)

# for i in range(len(combinations)):
#     print(i, combinations[i])

# print(combinations)

# print(full_MRDS_reduced)
      
# print(pl_MRDS_partitioned)

# # PART 2 (UMAP reduction)
# with open('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/document_testing/full_MRDS.pkl', 'rb') as handle:
#     df_refined_MRDS = pickle.load(handle)
# reduced_embedding_MRDS = global_embedding_reduction(df_refined_MRDS)
# df_reduced_embeddings_MRDS = pl.DataFrame(
#     reduced_embedding_MRDS
# )
# with open(path_dir + '/full_MRDS_reduced.pkl', 'wb') as handle:
#     pickle.dump(df_reduced_embeddings_MRDS, handle, protocol=pickle.HIGHEST_PROTOCOL)


# with open('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/document_testing/full_USMIN.pkl', 'rb') as handle:
#     df_refined_USMIN = pickle.load(handle)
# reduced_embedding_USMIN = global_embedding_reduction(df_refined_USMIN)
# df_reduced_embeddings_USMIN = pl.DataFrame(
#     reduced_embedding_USMIN
# )
# with open(path_dir + '/full_USMIN_reduced.pkl', 'wb') as handle:
#     pickle.dump(df_reduced_embeddings_USMIN, handle, protocol=pickle.HIGHEST_PROTOCOL)

# # PART 1 (Document forming)
# df_refined_MRDS = df_MRDS.select(
#     pl.struct(pl.all()).alias('data_as_struct')
# ).map_rows(
#     lambda x: (create_document(x, dict_MRDS))
# ).rename({'column_0':'embeddings'})

# with open(path_dir + '/full_MRDS.pkl', 'wb') as handle:
#     pickle.dump(df_refined_MRDS, handle, protocol=pickle.HIGHEST_PROTOCOL)

# df_refined_USMIN = df_USMIN.select(
#     pl.struct(pl.all()).alias('data_as_struct')
# ).map_rows(
#     lambda x: (create_document(x, dict_USMIN))
# ).rename({'column_0':'embeddings'})

# with open(path_dir + '/full_USMIN.pkl', 'wb') as handle:
#     pickle.dump(df_refined_USMIN, handle, protocol=pickle.HIGHEST_PROTOCOL)