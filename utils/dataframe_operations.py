from itertools import groupby
import polars as pl

from utils.load_files import as_dictionary

def clean_values(input_string:str, prefix:str|None=None, store_original_value:str|None=None):
    print(input_string, store_original_value)

    return 0

def map_attribute_labels(pl_rawdata, dict_attribute_map: dict):
    attr_in_rawdata = set(pl_rawdata.columns)
    attr_in_dictionary = set(dict_attribute_map.keys())

    attr_coexist = list(attr_in_rawdata & attr_in_dictionary)
    attr_exclusive = list(attr_in_dictionary - attr_in_rawdata)

    pl_mapped_data = pl_rawdata.select(
        pl.col(attr_coexist)
    )

    for attribute in attr_exclusive:
        attribute_label = dict_attribute_map[attribute]

        pl_mapped_data = pl_mapped_data.with_columns(
            pl.lit(attribute).cast(pl.Utf8).alias(attribute_label)
        )

        dict_attribute_map.pop(attribute)

    grouped_attribute_map = groupby(sorted(dict_attribute_map, key=dict_attribute_map.get), key=dict_attribute_map.get)

    multi_instance = {}
    for _, x in grouped_attribute_map:
        list_multilabels = list(x)
        if len(list_multilabels) > 1:
            multi_instance[dict_attribute_map[list_multilabels[0]]] = list_multilabels
            [dict_attribute_map.pop(key) for key in list_multilabels]

    pl_mapped_data = pl_mapped_data.rename(
        dict_attribute_map
    )

    for key, value in multi_instance.items():
        pl_mapped_data = pl_mapped_data.with_columns(
            pl.concat_str(
                pl.col(value).fill_null(""),
                separator=","
            ).alias(key)
        ).drop(
            value
        )

    return pl_mapped_data

def map_values(pl_rawdata, pl_value_map, 
               column_to_map: str, value_map_from: list|str, value_map_to: str, 
               prefix: str|None=None, store_original_value: str|None=None):

    if store_original_value:
        pl.col(column_to_map).alias(store_original_value)

    if isinstance(value_map_from, list):
        for i in value_map_from:
            dict_map = as_dictionary(pl_value_map, i, value_map_to)

            for old, new in dict_map.items():
                if new:
                    pl_rawdata = pl_rawdata.with_columns(
                        pl.col(column_to_map).str.replace(old, new)
                    )

    else:
        dict_map = as_dictionary(pl_value_map, value_map_from, value_map_to)
        for old, new in dict_map.items():
            if new:
                pl_rawdata = pl_rawdata.with_columns(
                    pl.col(column_to_map).str.replace(old, new)
                )

    # pl_rawdata = pl_rawdata.with_columns(
    #     pl.col(column_to_map).map_elements(clean_values, prefix, store_original_value)
    # )

    print(pl_rawdata)

    return pl_rawdata

def add_index_columns(pl_data, index_column_name:str, group_by_col:str|None=None):
    if group_by_col:
        pl_data = pl_data.group_by(
            group_by_col
        ).agg(
            [pl.all()]
        ).drop(group_by_col)


    list_indexes = list(range(pl_data.shape[0]))
    
    pl_data = pl_data.with_columns(
        pl.Series(list_indexes).alias(index_column_name)
    )

    try:
        return pl_data.explode(pl.exclude(index_column_name))
    except:
        return pl_data