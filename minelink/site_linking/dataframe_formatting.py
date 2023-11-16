import os
import pandas as pd

# from minelink.site_linking.column_mapping import *
from column_mapping import *

def create_dict_location(df_data):
    # location point, country (optional), state(optional), location_source_record_id, location_source, crs
    dict_location = df_data
    return dict_location

def create_dict_sameas(df_data):
    dict_sameas = df_data
    return dict_sameas

def separate_dataframe(df_data):
    filter_column(df_data)
    return df_data