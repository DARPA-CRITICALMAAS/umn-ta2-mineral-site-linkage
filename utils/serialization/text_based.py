# Serialization methods suggested in paper:

import polars as pl

def text_serialization(pl_data, unique_id_col:str, method:str, bool_leave_null=False, bool_shuffle=False):
    match method:
        case 'dfloader':
            pl_data = pl_data.select(
                pl.col(unique_id_col),
                serialized_string = pl.struct(pl.exclude(unique_id_col)).map_elements(lambda x: dfloader_style(x, bool_leave_null, bool_shuffle), return_dtype=pl.String)
            )

        case 'json':
            pl_data = pl_data.select(
                pl.col(unique_id_col),
                serialized_string = pl.struct(pl.exclude(unique_id_col)).map_elements(lambda x: json_style(x, bool_leave_null, bool_shuffle), return_dtype=pl.String)
            )

        case 'data_matrix':
            pl_data = pl_data.select(
                pl.col(unique_id_col),
                serialized_string = pl.struct(pl.exclude(unique_id_col)).map_elements(lambda x: data_matrix_style(x, bool_leave_null, bool_shuffle), return_dtype=pl.String)
            )

        case 'markdown':
            pl_data = pl_data.select(
                pl.col(unique_id_col),
                serialized_string = pl.struct(pl.exclude(unique_id_col)).map_elements(lambda x: markdown_style(x, bool_leave_null, bool_shuffle), return_dtype=pl.String)
            )

        case 'x_separated':
            pl_data = pl_data.select(
                pl.col(unique_id_col),
                serialized_string = pl.struct(pl.exclude(unique_id_col)).map_elements(lambda x: x_separated_style(x, bool_leave_null, bool_shuffle), return_dtype=pl.String)
            )

        case 'attribute_value_pairs':
            pl_data = pl_data.select(
                pl.col(unique_id_col),
                serialized_string = pl.struct(pl.exclude(unique_id_col)).map_elements(lambda x: attribute_value_pairs(x, bool_leave_null, bool_shuffle), return_dtype=pl.String)
            )

        case 'attribute_value_token':
            pl_data = pl_data.select(
                pl.col(unique_id_col),
                serialized_string = pl.struct(pl.exclude(unique_id_col)).map_elements(lambda x: attribute_value_token(x, bool_leave_null, bool_shuffle), return_dtype=pl.String)
            )

        case 'html':
            pl_data = pl_data.select(
                pl.col(unique_id_col),
                serialized_string = pl.struct(pl.exclude(unique_id_col)).map_elements(lambda x: html_style(x, bool_leave_null, bool_shuffle), return_dtype=pl.String)
            )

        case 'sentences':
            pl_data = pl_data.select(
                pl.col(unique_id_col),
                serialized_string = pl.struct(pl.exclude(unique_id_col)).map_elements(lambda x: sentence_style(x, bool_leave_null, bool_shuffle), return_dtype=pl.String)
            )

        case _:
            pass

    return pl_data

def dfloader_style(struct_attributes:dict, bool_leave_null, bool_shuffle) -> str:
    list_keys = list(struct_attributes.keys())
    

    print(struct_attributes.keys())
    return 0

def json_style(struct_attributes:dict, bool_leave_null, bool_shuffle) -> str:
    list_keys = list(struct_attributes.keys())
    
    serialized_string = '{'

    for k in list_keys:
        if struct_attributes[k]:
            serialized_string += f'"{k}": "{struct_attributes[k]}", '

    serialized_string = serialized_string.lstrip().rstrip().rstrip(',')
    serialized_string += '}'

    return serialized_string.lower()

def data_matrix_style(struct_attributes:dict, bool_leave_null, bool_shuffle) -> str:
    list_keys = list(struct_attributes.keys())
    

    print(struct_attributes.keys())
    return 0

def markdown_style(struct_attributes:dict, bool_leave_null, bool_shuffle) -> str:
    list_keys = list(struct_attributes.keys())
    

    print(struct_attributes.keys())
    return 0

def x_separated_style(struct_attributes:dict, bool_leave_null, bool_shuffle) -> str:
    list_keys = list(struct_attributes.keys())
    

    print(struct_attributes.keys())
    return 0

def attribute_value_pairs(struct_attributes:dict, bool_leave_null, bool_shuffle) -> str:
    list_keys = list(struct_attributes.keys())
    
    serialized_string = ''

    for k in list_keys:
        if struct_attributes[k]:
            serialized_string += f' {k}:{struct_attributes[k]} ;'

    serialized_string = serialized_string.lstrip().rstrip(';').rstrip()

    return serialized_string.lower()

def attribute_value_token(struct_attributes:dict, bool_leave_null, bool_shuffle) -> str:
    list_keys = list(struct_attributes.keys())
    
    serialized_string = ''

    for k in list_keys:
        if struct_attributes[k]:
            serialized_string += f'[ATT] {k} [VAL] {struct_attributes[k]} '

    serialized_string = serialized_string.rstrip()
    print(serialized_string)

    return serialized_string.lower()

def html_style(struct_attributes:dict, bool_leave_null, bool_shuffle) -> str:
    list_keys = list(struct_attributes.keys())

    print(struct_attributes.keys())
    return 0

def sentence_style(struct_attributes:dict, bool_leave_null, bool_shuffle) -> str:
    list_keys = list(struct_attributes.keys())

    serialized_string = ''

    for k in list_keys:
        if struct_attributes[k]:
            serialized_string += f' {k} is {struct_attributes[k]},'

    serialized_string = serialized_string.lstrip().rstrip(',')

    return serialized_string.lower()