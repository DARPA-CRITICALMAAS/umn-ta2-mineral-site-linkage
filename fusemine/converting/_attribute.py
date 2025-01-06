import random
from Typing import List, Dict

import polars as pl


def text_serialization(struct_attributes:Dict[str,str],
                       method:str,
                       bool_shuffle:bool=False) -> str:
    
    list_keys = list(struct_attributes.keys())
    if bool_shuffle:
        random.shuffle(list_keys)
    
    serialized_string = ''

    starting_token, middle_token, ending_token = special_tokens(method=method)

    for k in list_keys:
        if struct_attributes[k]:
            serialized_string += f'{starting_token}{k}{middle_token}{struct_attributes[k]}{ending_token}'

    # TODO: check if regex works here
    serialized_string = serialized_string.lstrip().rstrip(r'\W+').rstrip()

    return serialized_string

def special_tokens(method:str):
    if method == 'attribute_value_pairs':
        return ' ', ':', ' ;'
    
    elif method == 'attribute_value_token':
        return '[COL] ', ' [VAL] ', ' '