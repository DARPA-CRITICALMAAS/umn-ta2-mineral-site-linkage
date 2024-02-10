import pickle
import polars as pl

from make_output import *

path_dir = '/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/document_testing'

with open('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/document_testing/full_MRDS.pkl', 'rb') as handle:
    df_refined_MRDS = pickle.load(handle)
reduced_embedding_MRDS = global_embedding_reduction(df_refined_MRDS)
df_reduced_embeddings_MRDS = pl.DataFrame(
    reduced_embedding_MRDS
)
with open(path_dir + '/full_MRDS_reduced.pkl', 'wb') as handle:
    pickle.dump(df_reduced_embeddings_MRDS, handle, protocol=pickle.HIGHEST_PROTOCOL)


# with open('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/document_testing/part_USMIN.pkl', 'rb') as handle:
#     df_refined_USMIN = pickle.load(handle)
# reduced_embedding_USMIN = global_embedding_reduction(df_refined_USMIN)
# df_reduced_embeddings_USMIN = pl.DataFrame(
#     reduced_embedding_USMIN
# )
# with open(path_dir + '/part_USMIN_reduced.pkl', 'wb') as handle:
#     pickle.dump(df_reduced_embeddings_USMIN, handle, protocol=pickle.HIGHEST_PROTOCOL)

# df_refined_MRDS = df_MRDS.select(
#     pl.col(['site_name', 'commod1', 'commod2', 'commod3', 'ore', 'gangue'])
# )
# df_refined_USMIN =df_USMIN.select(
#     pl.col(['Ftr_Name', 'Other_Name', 'Commodity'])
# )

# df_refined_MRDS = df_MRDS.select(
#     pl.struct(pl.all()).alias('data_as_struct')
# ).map_rows(
#     lambda x: (create_document(x, dict_MRDS))
# ).rename({'column_0':'embeddings'})

# with open(path_dir + '/part_MRDS.pkl', 'wb') as handle:
#     pickle.dump(df_refined_MRDS, handle, protocol=pickle.HIGHEST_PROTOCOL)

# df_refined_USMIN = df_USMIN.select(
#     pl.struct(pl.all()).alias('data_as_struct')
# ).map_rows(
#     lambda x: (create_document(x, dict_USMIN))
# ).rename({'column_0':'embeddings'})

# with open(path_dir + '/part_USMIN.pkl', 'wb') as handle:
#     pickle.dump(df_refined_USMIN, handle, protocol=pickle.HIGHEST_PROTOCOL)