import regex as re
import polars as pl
import polars.selectors as cs

from minelink.params import *
from minelink.m0_load_input.load_data import load_file

def identify_unique_id(pl_data):
    pl_count = pl_data.select(
        pl.all().unique().count() == pl_data.shape[0]
    )

    potential_unique_id = [col.name for col in pl_count if col.all()]

    if len(potential_unique_id) == 0:
        return False
    
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

def identify_site_name(pl_data, remaining_columns):
    return 'site_name'

# df_remaining = df_data[list(col_available)]
#     df_remaining = df_remaining.select_dtypes(exclude=['number', 'bool'])
#     col_remaining = list(df_remaining.columns)

#     col_names = find_from_dictionary(df_dictionary, col_remaining, ['name'])

#     # TODO: concat columns with the col_names

#     return col_names

def identify_textual_location(remaining_columns):
    dict_text_loc = {}

    for i in remaining_columns:
        if i.lower()=='country':
            dict_text_loc[i] = 'country'
        elif re.search('state', i.lower()) or re.search('province', i.lower()):
            dict_text_loc[i] = 'state_or_province'

    return dict_text_loc

def identify_geo_location(pl_data, pl_dict, col_remaining):
    col_latitude = []
    col_longitude = []
    col_crs = []
    crs_value = ''

    pl_geo_loc = pl_data.select(
        pl.col(col_remaining)
    ).select(
        cs.by_dtype(pl.NUMERIC_DTYPES),
    )

    potential_geo_location = [col.name for col in pl_geo_loc]

    for i in potential_geo_location:
        if i.lower() == 'latitude':
            col_latitude.append(i)
        elif i.lower() == 'longitude':
            col_longitude.append(i)
        elif i.lower() == 'crs':
            col_longitude.append(i)

    def_latitude = 'something something in'
    crs_value = identify_crs(def_latitude)

    return col_latitude, col_longitude, crs_value

def identify_crs(def_latitude):
    list_crs = load_file([PATH_SRC_DIR], 'crs', '.pkl')

    list_tokens = re.split(' ', def_latitude)

    for token in list_tokens:
        if token in list_crs:
            return token

    return 'WGS84'

def identify_column(pl_data, pl_dict=None):
    remaining_columns = set(pl_data.columns)

    unique_id = identify_unique_id(pl_data)
    remaining_columns = remaining_columns - set([unique_id])

    dict_text_loc = identify_textual_location(remaining_columns)
    remaining_columns = remaining_columns - set(list(dict_text_loc.keys()))

    col_name = identify_site_name(pl_data, remaining_columns)

    latitude, longitude, crs = identify_geo_location(pl_data, pl_dict, remaining_columns)
    remaining_columns = remaining_columns - set(latitude) - set(longitude)

    remaining_columns = list(remaining_columns)

    return unique_id, col_name, latitude, longitude, crs, dict_text_loc, remaining_columns