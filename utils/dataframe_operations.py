import os
import time
import logging
import configparser

from itertools import groupby
import polars as pl

from utils.load_files import as_dictionary

config = configparser.ConfigParser()
config.read('./params.ini')
prefix_params = config['mapping.prefix']

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

def map_to_nodes(struct_input:dict, target_column:str, column_as:str, pl_map, value_map_to:str, bool_optional:bool, other_column_to_include:str|None=None,) -> int:
    attribute_item = struct_input[target_column]
    prefix = prefix_params['MINMOD_PREFIX']

    if attribute_item:
        formatted_item = []
        unique_nodes = []

        for i in attribute_item:
            if i:
                item = i.strip()
                
                # if not bool_optional:
                pl_item = pl_map.filter(
                    pl.col('map_from').list.contains(item.lower())
                )

                if pl_item.shape[0] == 0 and bool_optional:
                    formatted_item.append({
                        target_column:item
                    })

                elif pl_item.shape[0] != 0:
                    mapped_value = prefix + pl_item.item(0, value_map_to)
                    
                    if mapped_value not in unique_nodes:
                        if other_column_to_include:
                            formatted_item.append({
                                target_column:item,
                                column_as:mapped_value,
                                'reference':{'document':{'uri':struct_input[other_column_to_include]}}
                            })
                        else:
                            formatted_item.append({
                                target_column:item,
                                column_as:mapped_value
                            })
                        unique_nodes.append(mapped_value)

        return formatted_item

    return []

def map_values(pl_rawdata, pl_value_map, 
               column_to_map:str, map_column_as:str, value_map_from:list|str, value_map_to:list|str,
               prefix:str|None=None, store_original_value:str|None=None):
    
    if isinstance(value_map_to, list):
        pass

    dict_map = as_dictionary(pl_value_map, value_map_from, value_map_to)
    pl_rawdata = pl_rawdata.with_columns(
        pl.col(column_to_map).replace(dict_map)
    )

    return pl_rawdata

def map_node_values(pl_rawdata, pl_value_map, 
               column_to_map: str, map_column_as:str, value_map_from: list|str, value_map_to: str, bool_optional: bool,
               prefix: str|None=None, other_column_to_include:str|None=None, store_original_value: str|None=None):
    
    if store_original_value:
        pl_rawdata = pl_rawdata.with_columns(
            pl.col(column_to_map).alias(store_original_value)
        )
        col_to_drop = [column_to_map, store_original_value]
    else:
        store_original_value = column_to_map
        col_to_drop = [column_to_map]

    # if store_original_value:
    #     col_to_drop = [column_to_map, store_original_value]
    # else:
    #     col_to_drop = [column_to_map]

    if isinstance(value_map_from, list):
        pl_value_map = pl_value_map.with_columns(
            map_from = pl.concat_list(pl.col(value_map_from).str.to_lowercase())
        )

    else:
        pl_value_map = pl_value_map.rename(
            {
                value_map_from: 'map_from'
            }
        ).with_columns(
            pl.col('map_from').map_elements(lambda x: [x])
        )

    pl_value_map = pl_value_map.select(
        pl.col(['map_from', value_map_to])
    )

    if other_column_to_include:
        pl_rawdata = pl_rawdata.with_columns(
            tmp = pl.struct(pl.col([store_original_value, other_column_to_include])).map_elements(lambda x: map_to_nodes(x, store_original_value, map_column_as, pl_value_map, value_map_to, bool_optional, other_column_to_include))
        )
    else:
        pl_rawdata = pl_rawdata.with_columns(
            tmp = pl.struct(pl.col([store_original_value])).map_elements(lambda x: map_to_nodes(x, store_original_value, map_column_as, pl_value_map, value_map_to, bool_optional))
        )

    pl_rawdata = pl_rawdata.drop(
        col_to_drop
    )

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
    
def merge_in_align(pl_data1, pl_data2):
    return pl.concat(
        [pl_data1, pl_data2],
        how='align'
    )

def split_str_column(pl_data, column_name:str|None=None, bool_replace_numbers=False):
    if column_name:
        if not bool_replace_numbers:
            pl_data = pl_data.with_columns(
                pl.col(column_name).str.replace_all(r'[^A-Za-z0-9\(\)\s-]', ',').str.split(',')
            )

        else:
            pl_data = pl_data.with_columns(
                pl.col(column_name).str.replace_all(r'[^A-Za-z\s-]', ',').str.split(',')
            )

    return pl_data

