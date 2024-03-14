import polars as pl
from scipy import spatial
from itertools import combinations

import torch
from sentence_transformers import SentenceTransformer

# rb_model = 
st_model = SentenceTransformer('sentence-transformers/all-mpnet-base-v2')

def create_text_attribute_embedding(struct_attributes: dict) -> dict:
    """
    Converts the attribute value into textual embedding for every attribute that is available

    : param: struct_attributes = dictionary of extracted attributes for each mineral site record
    : return: dict_attribute_embeddings = dictionary of extracted attribute values converted into text embeddings
    """
    dict_attribute_embeddings = struct_attributes
    for key, value in dict_attribute_embeddings.items():
        dict_attribute_embeddings[key] = st_model.encode(value)
        print(dict_attribute_embeddings[key])

    return dict_attribute_embeddings

def compare_text_attribute_similarity(struct_attribute_embeddings: dict) -> dict:
    """
    
    : param: struct_attribute_embeddings = dictionary of extracted attribute values converted into text embeddings
    : return: dict_attribute_embeddings = 
    """
    # Getting the two required attributes
    # list_name_embedding = struct_attribute_embeddings['name']
    # list_commodity_embedding = struct_attribute_embeddings['commodities']

    # len_location_linked_groups = list(range(len(struct_attribute_embeddings[0])))
    # idx_combinations = combinations(len_location_linked_groups, 2)

    # for c in idx_combinations:
    #     attribute_similarity = 1 - spatial.distance.cosine(list_attribute_embedding[c[0]], list_attribute_embedding[c[1]])

    # mapping_dict = {}

    # list_intra_group = []
    # for i in list(mapping_dict.values()):
    #     group_code = str(struct_attribute_embeddings['GroupID'] + '_' + str(i))
    #     list_intra_group.append(group_code)

    # return {'intra_GroupID': list_intra_group}


    

# def get_cosine_similarity(dataset: dict) -> dict:
#     idx_list = dataset['idx']
#     name_embedding_list = dataset['name_embedding']
#     commod_embedding_list = dataset['commodity_embedding']

#     len_input = list(range(len(name_embedding_list)))
#     list_cosine_similarity = []

#     mapping_dict = {key: None for key in idx_list}
#     cosine_dict = {key: 0 for key in idx_list}

#     for c in combinations(len_input, 2):
#         name_similarity = 1 - spatial.distance.cosine(name_embedding_list[c[0]], name_embedding_list[c[1]])
#         if commod_embedding_list[c[0]]:
#             commod_similarity = 1 - spatial.distance.cosine(commod_embedding_list[c[0]], commod_embedding_list[c[1]])
#         # similarity = EMBEDDING_RATIO1 * name_similarity + (EMBEDDING_RATIO2) * commod_similarity + (1-EMBEDDING_RATIO1 - EMBEDDING_RATIO2) * other_similarity
#             similarity = EMBEDDING_RATIO1 * name_similarity + (1-EMBEDDING_RATIO1) * commod_similarity
#         else: 
#             similarity = name_similarity
#         list_cosine_similarity.append(similarity)

#         idx_first = idx_list[c[0]]
#         idx_second = idx_list[c[1]]

#         if mapping_dict[idx_first] is None:
#             mapping_dict[idx_first] = c[0]

#         if(similarity > THRESHOLD_SIMILARITY):
#             if(similarity > cosine_dict[idx_second]):
#                 mapping_dict[idx_second] = mapping_dict[idx_first]
#                 cosine_dict[idx_second] = similarity
#         elif mapping_dict[idx_second] is None:
#             mapping_dict[idx_second] = c[1]

#     new_group = []
#     for i in list(mapping_dict.values()):
#         group_code = str(dataset['GroupID']) + '_' + str(i)
#         new_group.append(group_code)

#     return {'idx': list(mapping_dict.keys()), 'new_group': new_group}

# def convert_to_embedding(mine_struct:dict) -> list:
#     mine_name = mine_struct['input']

#     model = SentenceTransformer('all-mpnet-base-v2')
#     txt_embed = model.encode(mine_name)
#     txt_embed = txt_embed.flatten().tolist()

#     return txt_embed

# def create_relevant_embeddings(alias_code:str, name_columns:list, commodity_columns:list):
#     pl_tolink_org = load_file([PATH_TMP_DIR, alias_code],
#                           'df_tolink',
#                           '.pkl')
    
#     pl_tolink_org = pl_tolink_org.sort('idx')

#     if commodity_columns:
#         pl_tolink = pl_tolink_org.select(
#             pl.col('idx'),
#             pl.col(name_columns).fill_null(" "),
#             pl.col(commodity_columns).fill_null(" ")
#         )

#         pl_commod = pl_tolink.select(
#             pl.col('idx'),
#             input = pl.concat_str(
#                 pl.col(commodity_columns),
#                 separator = ', '
#             )
#         )

#         pl_commod = pl_commod.with_columns(
#             commodity_embedding = pl.struct(pl.col('input')).map_elements(convert_to_embedding)
#         ).drop(
#             'input'
#         ).sort('idx')
        
#     else:
#         pl_tolink = pl_tolink_org.select(
#             pl.col('idx'),
#             pl.col(name_columns).fill_null(" "),
#         )

#         pl_commod = pl_tolink_org.select(
#             pl.col('idx'),
#             pl.Series('commodity_embedding', [[]], dtype=pl.List)
#         )

#     pl_name = pl_tolink.select(
#         pl.col('idx'),
#         all_names = pl.concat_str(
#             pl.col(name_columns),
#             separator = ', '
#         )
#     )

#     pl_name = pl_name.select(
#         pl.col('idx'),
#         pl.col('all_names').str.split(r'[^A-Za-z0-9\s]')
#     ).explode(
#         'all_names'
#     ).select(
#         pl.col('idx'),
#         input = pl.col('all_names').str.strip_chars().str.replace(r"(?i)property|mine|mines|prospect|prospects|properties|claim|claims|occurrence|occurrences|deposit|deposits", "")
#     )

#     pl_name = pl_name.with_columns(
#         name_embedding = pl.struct(pl.col('input')).map_elements(convert_to_embedding)
#     ).group_by(
#         'idx'
#     ).agg(
#         [pl.all()]
#     ).sort(
#         'idx'
#     ).drop(
#         ['input', 'idx']
#     )

#     pl_embeddings = pl.concat(
#         [pl_commod, pl_name],
#         how='horizontal'
#     )

#     save_ckpt(data=pl_embeddings,
#               list_path=[PATH_TMP_DIR, alias_code],
#               file_name='pl_embeddings')
    
#     return pl_embeddings



# def text_based_linking(pl_loclinked, alias_code:str, name_columns:list, commodity_columns:list):
#     pl_embeddings = create_relevant_embeddings(alias_code, name_columns, commodity_columns)

#     pl_loclinked = pl_loclinked.sort(
#         'idx'
#     ).drop(
#         'idx'
#     )

#     print(pl_loclinked.shape)

#     pl_embeddings = pl_embeddings.sort(
#         'idx'
#     )

#     pl_total = pl.concat(
#         [pl_embeddings, pl_loclinked],
#         how='horizontal'
#     ).explode(
#         'name_embedding'
#     ).drop_nulls(['idx'])

#     pl_individual = pl_total.filter(
#         pl.col('GroupID') == -1
#     ).drop(
#         'GroupID'
#     ).unique(
#         subset=['idx'], 
#         maintain_order=True
#     )

#     pl_grouped = pl_total.filter(
#         pl.col('GroupID') != -1
#     ).group_by(
#         'GroupID'
#     ).agg(
#         [pl.all()]
#     )

#     print(pl_grouped)

#     pl_grouped = pl_grouped.with_columns(
#         tmp = pl.struct(['idx', 'name_embedding', 'commodity_embedding', 'GroupID']).map_elements(get_cosine_similarity)
#     )

#     pl_grouped = pl_grouped.drop(
#         'idx',
#         'GroupID',
#         'name_embedding',
#         'commodity_embedding',
#     ).unnest(
#         'tmp'
#     ).explode(
#         pl.all()
#     ).group_by(
#         'new_group'
#     ).agg(
#         [pl.all()]
#     )

#     len_grouped = pl_grouped.shape[0]
#     pl_grouped = pl_grouped.with_columns(
#         GroupID = pl.Series(list(range(len_grouped))).cast(pl.Int64)
#     ).drop(
#         'new_group',
#     ).explode(
#         'idx'
#     )

#     len_individual = pl_individual.shape[0]
#     pl_individual = pl_individual.with_columns(
#         GroupID = pl.Series(list(range(len_grouped, len_grouped + len_individual))).cast(pl.Int64)
#     )

#     pl_individual = pl_individual.drop(
#         'name_embedding',
#         'commodity_embedding'
#     )

#     pl_total = pl.concat(
#         [pl_grouped, pl_individual],
#         how='vertical'
#     )

#     return pl_total


def text_based_linking(pl_locationlinked_mineralsites):
    pl_locationlinked_mineralsites = pl_locationlinked_mineralsites.head(10)

    pl_locationlinked_mineralsites = pl_locationlinked_mineralsites.with_columns(
        attribute_struct = pl.struct(pl.col(['name', 'commodities'])).map_elements(create_text_attribute_embedding)
    ).drop(
        'name', 'commodities'
    )

    print(pl_locationlinked_mineralsites)

    # pl_locationgrouped_mineralsite = pl_locationlinked_mineralsites.group_by(
    #     'GroupID'
    # ).agg(
    #     [pl.all()]
    # ).with_columns(
    #     text_based_grouping = pl.col('attribute_struct').map_elements(compare_text_attribute_similarity)
    # ).unnest(
    #     'attribute_struct'
    # )

    return 0