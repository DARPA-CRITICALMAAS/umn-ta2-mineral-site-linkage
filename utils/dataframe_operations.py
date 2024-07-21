import os
import time
import logging
import configparser

import ast
import binascii
import regex as re
from itertools import groupby
import polars as pl

from utils.load_files import as_dictionary

config = configparser.ConfigParser()
config.read('./params.ini')

prefix_params = config['mapping.prefix']
path_params = config['directory.paths']
mapping_columns = config['mapping.columns']

def map_attribute_labels(pl_rawdata, pl_attribute_map, key_column:str, value_column:str):
    attr_in_rawdata = set(pl_rawdata.columns)
    attr_in_dictionary = set(pl_attribute_map[key_column].to_list())

    attr_coexist = list(attr_in_rawdata & attr_in_dictionary)
    attr_exclusive = list(attr_in_dictionary - attr_in_rawdata)

    pl_mapped_data = pl_rawdata.select(
        pl.col(attr_coexist)
    )

    dict_attribute_map = as_dictionary(pl_attribute_map, key_column, value_column)

    for attribute in attr_exclusive:
        list_attribute_labels = pl_attribute_map.filter(
            pl.col(key_column) == attribute
        )[value_column].to_list()
        
        for idx, label in enumerate(list_attribute_labels):
            tmp_column_name = f'{label}_{binascii.hexlify(os.urandom(idx+10)).decode("utf-8")}'

            pl_mapped_data = pl_mapped_data.with_columns(
                pl.lit(attribute).cast(pl.Utf8).alias(tmp_column_name)
            )

            dict_attribute_map.update({tmp_column_name: label})

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

def append_to_list(list_to_be_modified:list, target_column:str, item:str, column_as:str,  mapped_value:str, bool_additional:bool, additional_item:str|list|None=None, additional_value:dict|str|list|None=None):
    if bool_additional:
        list_to_be_modified.append({
            target_column:item,
            column_as:mapped_value,
            additional_item: additional_value
        })

    else:
        list_to_be_modified.append({
            target_column:item,
            column_as:mapped_value,
            'confidence':1,
            'source':'raw_data_extraction'
        })

    return list_to_be_modified

def map_to_nodes(struct_input:dict, target_column:str, column_as:str, pl_map, value_map_to:str, bool_optional:bool, bool_code:bool, other_column_to_include:str|None=None,) -> int:
    attribute_item = struct_input[target_column]
    prefix = prefix_params['MINMOD_PREFIX']

    if attribute_item:
        formatted_item = []
        unique_nodes = []

        for i in attribute_item:
            if i:
                item = i.strip()
                identified_items = []

                if item.isupper() and bool_code:
                    # commodity code case
                    item = item.split(' ')
                    for i in item:
                        pl_item = pl_map.filter(
                            pl.col('map_from').list.contains(i.lower())
                        )
                        identified_items.append((i, pl_item))

                else:
                    pl_item = pl_map.filter(
                        pl.col('map_from').list.contains(item.lower())
                    )

                    identified_items.append((item, pl_item))

                for item, pl_item in identified_items:
                    if pl_item.shape[0] == 0 and bool_optional:
                        if other_column_to_include:
                            formatted_item = append_to_list(formatted_item, 
                                                            target_column, item, 
                                                            column_as, '', 
                                                            True, 'reference', {'document':{'uri':struct_input[other_column_to_include]}})

                        else:
                            formatted_item = append_to_list(formatted_item, 
                                                            target_column, item, 
                                                            column_as, '',
                                                            False)

                    elif pl_item.shape[0] == 1:
                        mapped_value = prefix + pl_item.item(0, value_map_to)
                        
                        if mapped_value not in unique_nodes:
                            if other_column_to_include:
                                formatted_item = append_to_list(formatted_item,
                                                                target_column, item,
                                                                column_as, mapped_value,
                                                                True, 'reference', {'document':{'uri':struct_input[other_column_to_include]}})
                            else:
                                formatted_item = append_to_list(formatted_item,
                                                                target_column, item,
                                                                column_as, mapped_value,
                                                                False)
                            unique_nodes.append(mapped_value)

        return formatted_item

    return []

def map_values(pl_rawdata, pl_value_map, 
               column_to_map:str, value_map_from:list|str, value_map_to:list|str,
               prefix:str|None=None, store_original_value:str|None=None):
    
    if isinstance(value_map_to, list):
        pass

    dict_map = as_dictionary(pl_value_map, value_map_from, value_map_to)
    pl_rawdata = pl_rawdata.with_columns(
        pl.col(column_to_map).replace(dict_map)
    )

    return pl_rawdata

def map_node_values(pl_rawdata, pl_value_map, 
               column_to_map: str, map_column_as:str, value_map_from: list|str, value_map_to: str, bool_optional: bool, bool_code=False,
               prefix: str|None=None, other_column_to_include:str|list|None=None, store_original_value: str|None=None):
    
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
        if isinstance(other_column_to_include, str):
            list_columns_to_struct = [store_original_value, other_column_to_include]

        pl_rawdata = pl_rawdata.with_columns(
            tmp = pl.struct(pl.col(list_columns_to_struct)).map_elements(lambda x: map_to_nodes(x, store_original_value, map_column_as, pl_value_map, value_map_to, bool_optional, bool_code, other_column_to_include))
        )

    else:
        pl_rawdata = pl_rawdata.with_columns(
            tmp = pl.struct(pl.col([store_original_value])).map_elements(lambda x: map_to_nodes(x, store_original_value, map_column_as, pl_value_map, value_map_to, bool_optional, bool_code))
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

def remove_space(list_items):
    cleaned_list = []
    for i in list_items:
        cleaned_list.append(i.strip())

    return cleaned_list

def split_str_column(pl_data, column_name:str|list|None=None, bool_replace_numbers=False):
    if column_name:
        if not bool_replace_numbers:
            pl_data = pl_data.with_columns(
                pl.col(column_name).str.replace_all(r'[^A-Za-z0-9\(\)\s-]', ',')
            )

        else:
            pl_data = pl_data.with_columns(
                pl.col(column_name).str.replace_all(r'[^A-Za-z\s-]', ',')
            )

        pl_data = pl_data.with_columns(
            pl.col(column_name).str.split(',').apply(remove_space)
        )

    return pl_data

def identify_list_items_overlap(struct_items:dict, target_list:list):
    values = list(struct_items.values())
    overlapping = list(set(values) & set(target_list))[0]

    return overlapping

def clean_to_null(struct_items:dict) -> dict:
    if not struct_items['commodity']:
        return {'commodity': None,
                'reference': None,}
    
    return struct_items

def data_to_none(input_object: dict, col_decision: str, col_affected: str|list, condition:str|None=None) -> dict:
    bool_need_to_none = False
    if condition:
        if input_object[col_decision] == condition:
            bool_need_to_none = True
    if not condition:
        if not input_object[col_decision]:
            bool_need_to_none = True

    if bool_need_to_none:
        input_object[col_decision] = None
        if isinstance(col_affected, list):
            for i in col_affected:
                input_object[i] = None
        else:
            input_object[col_affected] = None

    return input_object

def clean_nones(input_object: dict | list) -> dict | list:
    """
    Recursively remove all None values from either a dictionary or a list, and returns a new dictionary or list without the None values

    : param: input_object = either a dictionary or a list type that may or may not consist of a None value
    : return:= either a dictionary or a list type (same as the input) that does not consists of any None values
    """

    # List case
    if isinstance(input_object, list):
        return [clean_nones(x) for x in input_object if x is not None and x != ""]
    
    # Dictionary case
    elif isinstance(input_object, dict):
        cleaned_dict = {
            key: clean_nones(value)
            for key, value in input_object.items()
            if value and value is not None and value != ""
        }

        if not cleaned_dict:
            return None

        return cleaned_dict

    else:
        return input_object

########## NEW ##################
def normalize_dataframe(pl_data):
    for entity in ['deposit_type_candidate', 'state_or_province', 'country', 'crs', 'commodity']:
        try:
            pl_map = pl.read_csv(os.path.join(path_params['PATH_MAPFILE_DIR'], path_params[f'PATH_{entity.upper()}_MAPFILE']))
            if entity == 'commodity':
                pl_commod_code = pl.read_csv(os.path.join(path_params['PATH_RSRC_DIR'], path_params['PATH_MRDS_COMMODITY_CODE_FILE']))

                pl_map = pl.concat(
                    [pl_map, pl_commod_code],
                    how='align'
                )

            column_from = ast.literal_eval(mapping_columns[f'{entity.upper()}'])
            
            dict_map = {}
            if isinstance(column_from, str):
                column_from = [column_from]

            for i in column_from:
                pl_map = pl_map.with_columns(
                    pl.col(i).str.to_lowercase().str.strip_chars()
                )

                try:
                    dict_map.update(dict(zip(pl_map[i], pl_map['minmod_id'])))
                except:
                    try:
                        col_to_find = 'minmodid'

                        for i in list(pl_map.columns):
                            col_to_check = re.sub('[^a-zA-Z]', '', i).lower()

                            if col_to_find == col_to_check:
                                dict_map.update(dict(zip(pl_map[i], pl_map[i])))
                                break

                    except:
                        pass


            pl_data = pl_data.with_columns(
                pl.struct(pl.col(entity)).map_elements(lambda x: map_values(x, entity, dict_map), skip_nulls=False)
            )

        except:
            print(entity)
            pass

    return pl_data

def map_values(value_item:dict, schema_title:str, dict_map:dict) -> dict|list:
    entity_values = value_item[schema_title]
    
    items_in_list = []
    if isinstance(entity_values, list):
        list_entities = []
        items_in_list = []
        for v in entity_values:
            if not v:
                continue
            if v.strip() == '':
                continue

            dict_matchinfo, items_in_list = create_matchinfo(v, dict_map, items_in_list)

            if dict_matchinfo:
                list_entities.append(dict_matchinfo)

        if len(list_entities) == 0:
            return [{}]
        

        return list_entities
    
    item, items_in_list = create_matchinfo(entity_values, dict_map, items_in_list)

    return item

def create_matchinfo(observed_name:str, dict_map:dict, list_uris:list):
    if not observed_name or observed_name == '':
        return {
            'observed_name': None,
            'confidence': None,
            'source': None,
            'normalized_uri': None
        }, list_uris
    
    observed_name = observed_name.strip()

    normalized_uri, confidence = map_to_uri(observed_name.lower(), dict_map)

    dict_matchinfo = {
        'observed_name': observed_name,
        'confidence': confidence,
        'source': 'UMN Matching System v1',
    }

    if normalized_uri:
        if normalized_uri not in list_uris:
            dict_matchinfo.update({'normalized_uri': prefix_params['MINMOD_PREFIX'] + normalized_uri})
            list_uris.append(normalized_uri)
        else:
            dict_matchinfo = None
    else:
        dict_matchinfo.update({'normalized_uri': None})

    # dict_matchinfo = {
    #     k: v for k, v in dict_matchinfo.items() if v is not None
    # }

    return dict_matchinfo, list_uris

def map_to_uri(observed_name:str, dict_map:dict)->str:
    normalized_uri, confidence = exact_match_case(observed_name, dict_map)
    if normalized_uri:
        return normalized_uri, confidence

    # normalized_uri, confidence = fuzzy_match_case(observed_name)
    # if normalized_uri:
    #     return normalized_uri, confidence
    
    # return None, 0.5
    return None, confidence

def exact_match_case(observed_name:str, dict_map:dict):
    try:
        normalized_uri = dict_map[observed_name]
        return normalized_uri, 1.0
    
    except:
        return None, 1.0

def fuzzy_match_case(observed_name:str):
    normalized_uri = observed_name
    confidence = 0.7

    return normalized_uri, confidence