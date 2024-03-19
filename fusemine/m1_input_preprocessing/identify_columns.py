import regex as re
import polars as pl
import polars.selectors as cs
import numpy as np
from sentence_transformers import SentenceTransformer, util

from fusemine.params import *
from fusemine.m0_load_input.load_data import load_file

def compare_dictionary(dict_target, dict_against):
    model = SentenceTransformer('all-MiniLM-L6-v2')

    name_target = list(dict_target.keys())
    descrip_target = list(np.array(list(dict_target.values())).flatten())
    emb_target = model.encode(descrip_target, convert_to_tensor=True)

    name_against = list(dict_against.keys())
    descrip_against = list(np.array(list(dict_against.values())).flatten())
    emb_against = model.encode(descrip_against, convert_to_tensor=True)

    cosine_scores = util.cos_sim(emb_target, emb_against)
    cosine_scores = np.array(cosine_scores.tolist())

    idx = list(dict.fromkeys(np.where(cosine_scores > 0.45)[1]))

    col_match = np.array(name_against)[idx]

    return col_match

def compare_description(dict_data, dict_target, col_candidates, col_target):
    col_candidates = list(set(col_candidates) & set(list(dict_data.keys())))
    if len(col_candidates) == 0:
        return []

    dict_candidates = pl.from_dict(dict_data).select(
        pl.col(col_candidates)
    ).to_dict(as_series=False)

    dict_target = pl.from_dict(dict_target).select(
        pl.col(col_target)
    ).to_dict(as_series=False)

    col_match = compare_dictionary(dict_target, dict_candidates)

    return col_match

def identify_unique_id(pl_data, dict_data):
    pl_count = pl_data.select(
        pl.all().unique().count() == pl_data.shape[0]
    )

    potential_unique_id = [col.name for col in pl_count if col.all()]

    if len(potential_unique_id) == 0:
        return False
    elif len(potential_unique_id) == 1:
        return potential_unique_id[0]   # maybe need to check with the attribute dictionary too
    
    for i in potential_unique_id:
        splitted_col_name = re.split('[^A-Za-z]', i.lower())
        unique_label = ''.join(re.findall('[A-Za-z]', i))

        if 'id' in splitted_col_name:
            return i

    #     if unique_label not in dict_dictionary.keys():
    #         continue

    #     elif re.search('unique', dict_dictionary[unique_label].lower()):
    #         return i
        
        # else:
        #     return False

    return False

def identify_site_name(pl_data, remaining_columns, dict_data, dict_target):
    remaining_columns = list(pl_data.columns)
    pl_name = pl_data.select(
        pl.col(remaining_columns)
    ).select(
        ~cs.by_dtype(pl.NUMERIC_DTYPES, pl.Boolean)
    )

    potential_name = pl_name.columns
    col_match = compare_description(dict_data, dict_target, potential_name, ['name'])
    
    potential = set(remaining_columns) & set(['Ftr_Name', 'Site_Name', 'site_name', 'Site'])
    potential = list(set(col_match) | potential)
    
    return potential[0]

def identify_textual_location(remaining_columns):
    dict_text_loc = {}

    for i in remaining_columns:
        if i.lower()=='country':
            dict_text_loc[i] = 'country'
        elif re.search('state', i.lower()) or re.search('province', i.lower()):
            dict_text_loc[i] = 'state_or_province'

    return dict_text_loc

def identify_geo_location(pl_data, remaining_columns, dict_data, dict_target):
    col_latitude = []
    col_longitude = []
    col_crs = []
    crs_value = ''

    pl_data_remaining = pl_data.select(
        pl.col(remaining_columns)
    )

    pl_geo_loc = pl_data_remaining.select(
        cs.by_dtype(pl.NUMERIC_DTYPES),
    )

    pl_crs = pl_data_remaining.select(
         ~cs.by_dtype(pl.NUMERIC_DTYPES, pl.Boolean),
    )

    potential_geo_location = [col.name for col in pl_geo_loc]
    potential_crs = [col.name for col in pl_crs]

    for i in potential_geo_location:
        if re.search('latitude', i.lower()):
            col_latitude.append(i)
        elif re.search('longitude', i.lower()):
            col_longitude.append(i)
        elif re.search('approx_lon', i.lower()):
            col_longitude.append(i)
        elif re.search('approx_lat', i.lower()):
            col_latitude.append(i)

    for i in potential_crs:
        if i.lower() == 'crs':
            col_longitude.append(i)

    potential_geo_loc = list(set(pl_geo_loc.columns) - set(col_latitude) - set(col_longitude))
    if len(potential_geo_loc) != 0:
        col_match = compare_description(dict_data, dict_target, potential_geo_loc, ['latitude', 'longitude'])

        for i in col_match:
            if re.search('lat', i.lower()):
                col_latitude.append(i)
            elif re.search('long', i.lower()):
                col_longitude.append(i)

    # Delete later
    # col_latitude = list(set(['Lat_WGS84', 'latitude', 'Latitude']) & set(pl_geo_loc.columns))
    # col_longitude = list(set(['Long_WGS84', 'longitude', 'Longitude']) & set(pl_geo_loc.columns))
    
    potential_crs = list(set(pl_crs.columns) - set(col_crs))
    if len(potential_crs) != 0:
        col_crs = compare_description(dict_data, dict_target, potential_crs, ['crs'])

    print(col_latitude, col_longitude, crs_value)

    if len(col_crs) != 0:
        rep_crs = col_crs[0]
        crs_value = pl_crs.select(
            pl.first(rep_crs)
        ).item()
    else:
        rep_latitude = col_latitude[0]
        crs_value = identify_crs(rep_latitude)

        def_latitude = dict_data[rep_latitude]
        crs_value = identify_crs(def_latitude)
        print(crs_value)

    if dict_data == None:
        return col_latitude, col_longitude, crs_value

    return col_latitude, col_longitude, crs_value

def identify_crs(def_latitude):
    list_crs = load_file([PATH_SRC_DIR], 'crs', '.pkl')

    list_tokens = re.split(' ', def_latitude)

    for token in list_tokens:
        if token in list_crs:
            return token

    return 'WGS84'

def identify_column(pl_data, dict_data=None):
    dict_target = load_file([PATH_SRC_DIR],
                            'dictionary_target',
                            '.pkl')
    
    remaining_columns = set(pl_data.columns)

    unique_id = identify_unique_id(pl_data, dict_data)
    remaining_columns = remaining_columns - set([unique_id])

    dict_text_loc = identify_textual_location(remaining_columns)
    remaining_columns = remaining_columns - set(list(dict_text_loc.keys()))

    col_name = identify_site_name(pl_data, remaining_columns, dict_data, dict_target)
    remaining_columns = remaining_columns - set(col_name)

    latitude, longitude, crs = identify_geo_location(pl_data, remaining_columns, dict_data, dict_target)
    remaining_columns = remaining_columns - set(latitude) - set(longitude)

    remaining_columns = list(remaining_columns)

    return unique_id, col_name, latitude, longitude, crs, dict_text_loc, remaining_columns