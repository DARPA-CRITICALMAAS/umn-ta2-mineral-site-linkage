import polars as pl

from minelink.params import *
from minelink.m0_load_input.load_data import load_file

from sentence_transformers import SentenceTransformer

# def embedding_reduction(txt_embed):


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

def text_based_linking(alias_code):
    pl_tolink = load_file([PATH_TMP_DIR, alias_code],
                          'df_tolink',
                          '.pkl')
    dictionary = load_file([PATH_TMP_DIR, alias_code],
                           'mini_dictionary',
                           '.pkl')
    
    pl_idx = pl_tolink.select(
        pl.col('idx')
    )
    pl_tolink = pl_tolink.drop('idx').with_columns(
        pl.when(pl.all() == None)
        .then(pl.lit(' '))
        .otherwise(pl.all())
        .name.keep()
    )

    # TODO: remove later
    # pl_idx = pl_idx.head(4)
    # pl_tolink = pl_tolink.head(4)

    pl_doc = pl_tolink.select(
        pl.struct(pl.all()).alias('data_as_struct')
    ).map_rows(
        lambda x: (create_document(x, dictionary))
    ).rename({'column_0':'embeddings'})

    pl_doc = pl.concat([pl_idx, pl_doc], how='horizontal')

    # print(pl_doc)

    # .rename(
    #     {
    #         'column_0':'embeddings'
    #     }
    # )
    
    # create_document(pl_d, dictionary)
    # print(pl_doc)
    return pl_doc