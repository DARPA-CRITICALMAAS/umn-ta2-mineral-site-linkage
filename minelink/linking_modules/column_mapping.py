import pandas as pd

def df_to_dict(df_data_dic):
    dict_description = df_data_dic
    return dict_description

def find_crs(list_sent_geo):
    """

    input: list_sent_geo = list of sentences that describes latitude and longitudes
    return: crs = string representing the CRS value (either in CRS or EPSG)
    """

    for item in list_sent_geo:
        crs = 'NAD83'

        return crs
    
    return 'WGS84'  # If cannot find an acceptable form of crs just return WGS84

# def find_geo_info(df_data_dic):
#     """
#     return: list of columns representing geo-location relevant information (latitude, longitude, crs)
#     """

#     list_geo = []

#     return list_geo

def find_name_geom_columns(df_data, df_dic):
    """
    input: df_data = dataframe 
    input: df_dic = dataframe
    """
    # dict_description = df_to_dict(df_dic)

    try:
        crs = df_data.crs
        lat = df_data.geometry.y
        long = df_data.geometry.x

        # If there are any columns matching the values of crs, lat, long drop them from df_dic

        bool_geometry = True
        
    except:
        # Convert dictionary dataframe into form of dictionary and convert them to BERT embeddings
        print("None")
        bool_geometry = False

    col_name = 'name'

    # Needs to be longitude, latitude, crs
    list_col_geom = [long, lat, crs]

    selection_threshold = 0.4

    return col_name, list_col_geom, bool_geometry

# def filter_column(df_data_dic):
#     # dict_description = df_to_dict(df_data_dic)

#     print(df_data_dic['label', 'short_desc', 'description'])

#     list_col_drop = []
#     return list_col_drop

# def map_column(df_data_dic, list_elim):
#     df_return = df_data_dic

#     return df_return