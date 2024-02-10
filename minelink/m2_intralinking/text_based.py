import polars as pl
from scipy import spatial
from itertools import combinations
from sentence_transformers import SentenceTransformer

from minelink.params import *
from minelink.m0_load_input.load_data import load_file
from minelink.m0_load_input.save_ckpt import save_ckpt

def convert_to_embedding(mine_struct:dict) -> list:
    mine_name = mine_struct['input']

    model = SentenceTransformer('all-mpnet-base-v2')
    txt_embed = model.encode(mine_name)
    txt_embed = txt_embed.flatten().tolist()

    return txt_embed

def create_relevant_embeddings(alias_code:str, name_columns:list, commodity_columns:list):
    pl_tolink = load_file([PATH_TMP_DIR, alias_code],
                          'df_tolink',
                          '.pkl')
    
    pl_tolink = pl_tolink.sort('idx')

    pl_tolink = pl_tolink.select(
        pl.col('idx'),
        pl.col(name_columns).fill_null(" "),
        pl.col(commodity_columns).fill_null(" ")
    )

    pl_name = pl_tolink.select(
        pl.col('idx'),
        all_names = pl.concat_str(
            pl.col(name_columns),
            separator = ', '
        )
    )

    pl_commod = pl_tolink.select(
        pl.col('idx'),
        input = pl.concat_str(
            pl.col(commodity_columns),
            separator = ', '
        )
    )

    pl_commod = pl_commod.with_columns(
        commodity_embedding = pl.struct(pl.col('input')).map_elements(convert_to_embedding)
    ).drop(
        'input'
    ).sort('idx')

    pl_name = pl_name.select(
        pl.col('idx'),
        pl.col('all_names').str.split(r'[^A-Za-z0-9\s]')
    ).explode(
        'all_names'
    ).select(
        pl.col('idx'),
        input = pl.col('all_names').str.strip_chars().str.replace(r"(?i)property|mine|mines|prospect|prospects|properties|claim|claims|occurrence|occurrences|deposit|deposits", "")
    )

    pl_name = pl_name.with_columns(
        name_embedding = pl.struct(pl.col('input')).map_elements(convert_to_embedding)
    ).group_by(
        'idx'
    ).agg(
        [pl.all()]
    ).sort(
        'idx'
    ).drop(
        ['input', 'idx']
    )

    pl_embeddings = pl.concat(
        [pl_commod, pl_name],
        how='horizontal'
    )

    save_ckpt(data=pl_embeddings,
              list_path=[PATH_TMP_DIR, alias_code],
              file_name='pl_embeddings')
    
    return pl_embeddings

def get_cosine_similarity(dataset: dict) -> dict:
    idx_list = dataset['idx']
    name_embedding_list = dataset['name_embedding']
    commod_embedding_list = dataset['commodity_embedding']
    # other_embedding_list = dataset['other_embedding']

    len_input = list(range(len(name_embedding_list)))
    list_cosine_similarity = []

    mapping_dict = {key: None for key in idx_list}
    cosine_dict = {key: 0 for key in idx_list}

    for c in combinations(len_input, 2):
        name_similarity = 1 - spatial.distance.cosine(name_embedding_list[c[0]], name_embedding_list[c[1]])
        commod_similarity = 1 - spatial.distance.cosine(commod_embedding_list[c[0]], commod_embedding_list[c[1]])
        # other_similarity = 1 - spatial.distance.cosine(other_embedding_list[c[0]], other_embedding_list[c[1]])

        # similarity = EMBEDDING_RATIO1 * name_similarity + (EMBEDDING_RATIO2) * commod_similarity + (1-EMBEDDING_RATIO1 - EMBEDDING_RATIO2) * other_similarity
        similarity = EMBEDDING_RATIO1 * name_similarity + (1-EMBEDDING_RATIO1) * commod_similarity
        list_cosine_similarity.append(similarity)

        idx_first = idx_list[c[0]]
        idx_second = idx_list[c[1]]

        if mapping_dict[idx_first] is None:
            mapping_dict[idx_first] = c[0]

        if(similarity > THRESHOLD_SIMILARITY):
            if(similarity > cosine_dict[idx_second]):
                mapping_dict[idx_second] = mapping_dict[idx_first]
                cosine_dict[idx_second] = similarity
        elif mapping_dict[idx_second] is None:
            mapping_dict[idx_second] = c[1]

    new_group = []
    for i in list(mapping_dict.values()):
        group_code = str(dataset['GroupID']) + '_' + str(i)
        new_group.append(group_code)

    return {'idx': list(mapping_dict.keys()), 'new_group': new_group}

def text_based_linking(pl_loclinked, alias_code:str, name_columns:list, commodity_columns:list):
    pl_embeddings = create_relevant_embeddings(alias_code, name_columns, commodity_columns)

    pl_loclinked = pl_loclinked.sort(
        'idx'
    ).drop(
        'idx'
    )
    pl_embeddings = pl_embeddings.sort(
        'idx'
    )

    pl_total = pl.concat(
        [pl_embeddings, pl_loclinked],
        how='horizontal'
    ).explode(
        'name_embedding'
    )

    pl_individual = pl_total.filter(
        pl.col('GroupID') == -1
    ).drop(
        'GroupID'
    ).unique(
        subset=['idx'], 
        maintain_order=True
    )

    pl_grouped = pl_total.filter(
        pl.col('GroupID') != -1
    ).group_by(
        'GroupID'
    ).agg(
        [pl.all()]
    )

    pl_grouped = pl_grouped.with_columns(
        tmp = pl.struct(['idx', 'name_embedding', 'commodity_embedding', 'GroupID']).map_elements(get_cosine_similarity)
    )

    pl_grouped = pl_grouped.drop(
        'idx',
        'GroupID',
        'name_embedding',
        'commodity_embedding',
    ).unnest(
        'tmp'
    ).explode(
        pl.all()
    ).group_by(
        'new_group'
    ).agg(
        [pl.all()]
    )

    len_grouped = pl_grouped.shape[0]
    pl_grouped = pl_grouped.with_columns(
        GroupID = pl.Series(list(range(len_grouped))).cast(pl.Int64)
    ).drop(
        'new_group',
    ).explode(
        'idx'
    )

    len_individual = pl_individual.shape[0]
    pl_individual = pl_individual.with_columns(
        GroupID = pl.Series(list(range(len_grouped, len_grouped + len_individual))).cast(pl.Int64)
    )

    pl_individual = pl_individual.drop(
        'name_embedding',
        'commodity_embedding'
    )

    pl_total = pl.concat(
        [pl_grouped, pl_individual],
        how='vertical'
    )

    return pl_total