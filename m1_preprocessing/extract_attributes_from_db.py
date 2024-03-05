import os
import pickle
import configparser

import regex as re
import numpy as np
import polars as pl
import polars.selectors as cs
from sentence_transformers import SentenceTransformer, util

from m0_loading_and_saving.load_local_data import open_local_files
from m1_preprocessing.compare_attribute_def_similarity import find_similar_attributes

config = configparser.ConfigParser()
config.read('./params.ini')
path_params = config['directory.paths']
ATTRIBUTE_DEF_SIMILARITY_THRESHOLD = float(config['preprocessing.params']['ATTRIBUTE_DEFINITION_THRESHOLD'])

model = SentenceTransformer('sentence-transformers/all-mpnet-base-v2')

def identify_id_attribute(pl_mineralsite, list_known_mappings:list):
    """
    Identifies the attribute (column) that is representing the unique ID of the mineral site record.
    If there is no unique ID, it will provide a number for the mineral site

    : param: pl_mineralsite = polars dataframe of all the mineral site records
    : param: list_known_mappings = list consisting of previous column labels that mapped to unique id
    : return list_updated_known_mappings = list of updated column labels that mapped to unique id
    : return: pl_updated_mineralsite: polars dataframe with modified attribute labels
    """

    # Gives a dataframe that only has unique items
    pl_unique_column = pl_mineralsite.select(
        pl.all().unique().count() == pl_mineralsite.shape[0]
    )
    list_potential_id = [col.name for col in pl_unique_column if col.all()]

    list_updated_known_mappings = list_known_mappings

    if len(list_potential_id) == 0:     # Case where there is no column with all unique items
        list_recordid_numbering = list(range(1, pl_mineralsite.shape[0] + 1))
        pl_updated_mineralsite = pl_mineralsite.with_columns(
            record_id = pl.Series(list_recordid_numbering)
        )
    
    elif len(list_potential_id) == 1:   # Caase where there is one column with all unique items
        pl_updated_mineralsite = pl_mineralsite.rename(
            {'record_id': list_potential_id[0]}
        )
        list_updated_known_mappings.extend(list_potential_id)

    else:
        list_unique_id = []
        for i in list_potential_id:
            splitted_attribute_label = re.aplit('[^A-Za-z]', c.lower())

            if 'id' in splitted_attribute_label:
                list_unique_id.append(i)

        pl_updated_mineralsite = pl_mineralsite.with_columns(
            record_id = pl.concat_str(
                pl.col(list_unique_id).fill_null("")
            )
        ).drop(
            list_unique_id
        )
        list_updated_known_mappings.extend(list_unique_id)
    
    return list_updated_known_mappings, pl_updated_mineralsite

def identify_name_attribute(pl_mineralsite, dict_attributes, list_known_mappings:list):
    """
    Identifies the attribute that is representing the name and other name (i.e., name alias) of the mineral site
    If there are no name, it will be named 'Unknown'

    : param: pl_mineralsite = polars dataframe of all the mineral site records
    : param: dict_attributes = dictionary that gives label and its definition
    : param: list_known_mappings = list consisting of previous column labels that mapped to unique id
    : return list_updated_known_mappings = list of updated column labels that mapped to unique id
    : return: pl_updated_mineralsite: polars dataframe with modified attribute labels
    """
    # Selects columns that has 'string' type attribute
    pl_string_data = pl_mineralsite.select(
        ~cs.by_dtype(pl.NUMERIC_DTYPES, pl.Boolean)
    )
    set_string_based_attributes = set(list(pl_string_data.columns))

    # Check if any of the attribute labels in pl_mineralsite matches ones in known mappings
    list_attributes_known = set(list_known_mappings) & set_string_based_attributes

    name_definition = "Current name of the site" 
    set_potential_name_attribute = find_similar_attributes(name_definition, dict_attributes)

    list_updated_known_mappings = list(list_attributes_known | set_potential_name_attribute)

    # Update the mineralsite dataframe to combine all possible names
    pl_updated_mineralsite = pl_mineralsite.with_columns(
        name = pl.concat_str(
            pl.col(list_updated_known_mappings).fill_null(""),
            separator=", "
        )
    ).drop(
        list_updated_known_mappings
    )

    return list_updated_known_mappings, pl_updated_mineralsite

def identify_geolocation_attribute(pl_mineralsite, dict_attributes, list_known_mappings:list):
    """
    Identifies the attribute that is representing the latitude and longitude (and crs) of the mineral site
    If crs is not available as an independent attribute, it will try to identify the crs through string match on the latitude label and latitude definition
    If there is no attributes representing such, it will return a None item

    : param: pl_mineralsite = polars dataframe of all the mineral site records
    : param: dict_attributes = dictionary that gives label and its definition
    : param: list_known_mappings = list consisting of previous column labels that mapped to unique id
    : return list_updated_known_mappings = list of updated column labels that mapped to unique id
    : return: pl_updated_mineralsite: polars dataframe with modified attribute labels
    """

    latitude_definition = "Latitude in decimal degree"
    set_potential_lat_attribute = find_similar_attributes(latitude_definition, dict_attributes)
    list_updated_latitude_mappings = list(set_potential_lat_attribute | list_known_mappings[0])

    longitude_definition = "Longitude in decimal degree"
    set_potential_long_attribute = find_similar_attributes(longitude_definition, dict_attributes)
    list_updated_longitude_mappings = list(set_potential_long_attribute | list_known_mappings[1])

    print(list_updated_latitude_mappings, list_updated_longitude_mappings)

    crs_definition = "Coordinate reference system"
    list_potential_crs_attribute = list(find_similar_attributes(crs_definition, dict_attributes))

    list_possible_crs = open_local_files(path_params['PATH_RESOURCE_DIR'], 'crs', '.pkl')
    identified_latitude_label = list_updated_latitude_mappings[0]
    identified_latitude_definition = dict_attributes[identified_latitude_label]

    for token in re.split(' ', identified_latitude_definition):
        if token in list_possible_crs:
            identified_crs = token

    return [list_updated_latitude_mappings, list_updated_longitude_mappings, identified_crs]

def identify_textlocation_attribute(pl_mineralsite):
    """
    Identifies the attribute that is representing the country and/or state of the mineral site

    : param: pl_mineralsite = polars dataframe of all the mineral site records
    : return: pl_updated_mineralsite: polars dataframe with modified attribute labels
    """
    dict_textlocation_attributes = {}

    for i in list(pl_mineralsite.columns):
        if i.lower() == 'country':
            dict_textlocation_attributes[i] = 'country'
        elif re.search('state', i.lower()) or re.search('province', i.lower()):
            dict_textlocation_attributes[i] = 'state_or_province'

    pl_updated_mineralsite = pl_mineralsite.rename(
        dict_textlocation_attributes
    )

    return pl_updated_mineralsite

# TODO: Update to include these three
def identify_commodity_attribute(pl_mineralsite, dict_attributes, list_known_mappings:list):
    """
    Identifies attribute(s) representing the commodity available at the mineral site

    : param: pl_mineralsite:
    : param: dict_known_mappings (default=None) = 
    : return: 
    """
    commodity_definition = "Commodities available at the mineral site"
    set_potential_lat_attribute = find_similar_attributes(commodity_definition, dict_attributes)

def identify_operationtype_attribute(pl_mineralsite, dict_attributes, list_known_mappings:list):
    """
    
    """
    operationtype_definition = "Current operation status of the mineral site."
    set_potential_lat_attribute = find_similar_attributes(operationtype_definition, dict_attributes)

def identify_recordyear_attribute(pl_mineralsite, dict_attributes, list_known_mappings:list):
    """
    
    """
    recordyear_definition = "Year the mineral site record was taken"
    set_potential_lat_attribute = find_similar_attributes(recordyear_definition, dict_attributes)

####

def map_attribute_labels(pl_mineralsite, dict_attributes):
    """
    
    : param: pl_mineralsite = polars dataframe of the mineralsite record
    : param: dict_attributes = dictionary that gives label and its definition
    : return: pl_processed_mineralsite = polars dataframe with the attribute label mapped to the ones we have defined
    """

    dict_known_attribute_maps = open_local_files(path_params['PATH_RESOURCE_DIR'], 'attribute_dictionary', '.pkl')

    # map attribute label representing unique id as 'record_id'
    list_known_recordid = dict_known_attribute_maps['record_id']
    updated_list_known_recordid, pl_mineralsite = identify_id_attribute(pl_mineralsite, list_known_recordid)
    dict_known_attribute_maps['record_id'] = updated_list_known_recordid

    # map attribute label representing site name as 'name', 'other_names'
    list_known_name = dict_known_attribute_maps['name']
    updated_list_known_name, pl_mineralsite = identify_name_attribute(pl_mineralsite, dict_attributes, list_known_name)
    dict_known_attribute_maps['name'] = updated_list_known_name

    # map attribute label reprepresenting geolocation (latitude, longtide, crs)
    list_known_latitude = dict_known_attribute_maps['latitude']
    list_known_longitude = dict_known_attribute_maps['longitude']
    updated_list_known_location, pl_mineralsite = identify_name_attribute(pl_mineralsite, dict_attributes, [list_known_latitude, list_known_longitude])
    dict_known_attribute_maps['latitude'] = updated_list_known_location[0]
    dict_known_attribute_maps['longitude'] = updated_list_known_location[1]

    # # map attribute label representing longitude and latitude as 'location'
    # if 'location' in known_attribute_map:
    #     print("make a list of name known mapping")
    # identify_geolocation_attribute(pl_mineralsite, list_known_mappings:list)

    # # identify available attribute labels representing textual location
    # if 'location' in known_attribute_map:
    #     print("make a list of name known mapping")
    # identify_textlocation_attribute(pl_mineralsite, list_known_mappings:list)

    # # identify available attribute labels representing textual location
    # if 'operation_type' in known_attribute_map:
    #     print("make a list of name known mapping")
    # identify_operationtype_attribute(pl_mineralsite, list_known_mappings:list)

    # if 'record_year' in known_attribute_map:
    #     print("make a list of name known mapping")
    # identify_recordyear_attribute(pl_mineralsite, list_known_mappings:list)


    # pl_processed_mineralsite = pl_mineralsite

    # # Storing the updated known attribute dictionary to the resource file
    # with open(os.path.join(path_params['PATH_RESOURCE_DIR'], 'attribute_dictionary.pkl'), 'wb') as handle:
    #     pickle.dump(dict_known_attribute_maps, handle, protocol=pickle.HIGHEST_PROTOCOL)

    # return pl_processed_mineralsite