import pickle
import polars as pl

from sentence_transformers import SentenceTransformer

from make_output import *

def individual_embedding(struct_data):
    model = SentenceTransformer('all-mpnet-base-v2')
    txt_embeded = model.encode(struct_data)

    print(txt_embeded)

    return 0

path_dir = '/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/document_testing'

df_individual_MRDS = df_MRDS.head(4)
print(df_individual_MRDS)

# df_individual_MRDS = df_MRDS.select(
#     pl.struct(pl.all()).alias('data_as_struct')
# )
# slice_MRDS = df_individual_MRDS.head(4).map_rows(
#     lambda x: (individual_embedding(x))
# )


# print(slice_MRDS)
# model = SentenceTransformer('all-mpnet-base-v2')
# txt_embed = model.encode(document)

# model = SentenceTransformer('all-mpnet-base-v2')
#     txt_embed = model.encode(document)
#     txt_embed = txt_embed.flatten().tolist()

#     return (txt_embed, )

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