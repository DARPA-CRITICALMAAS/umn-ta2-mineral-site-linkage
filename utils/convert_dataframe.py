import polars as pl
import pandas as pd
import geopandas as gpd
from shapely import wkt

def to_polars(df_input, input_dataframe_type: str):
    match input_dataframe_type:
        case 'pd':
            return pl.from_pandas(df_input)
        
        case 'gpd':
            pd_input = to_pandas(df_input, 'gpd')
            return pl.from_pandas(pd_input)
        
        case _:
            return df_input

def to_pandas(df_input, input_dataframe_type: str):
    match input_dataframe_type:
        case 'pl':
            return df_input.to_pandas()
        
        case 'gpd':
            try:
                df_input['location'] = df_input['location'].apply(wkt.dumps)
                return df_input
            except:
                return pd.DataFrame(df_input)
        
        case _:
            return df_input

def to_geopandas(df_input, input_dataframe_type: str, geometry_column='location'):
    match input_dataframe_type:
        case 'gpd':
            return df_input
        
        case _:
            pd_input = to_pandas(df_input, input_dataframe_type)
            crs_value = pd_input['crs'][0]

            try:
                pd_input[geometry_column] = pd_input[geometry_column].apply(wkt.loads)

                return gpd.GeoDataFrame(
                    pd_input,
                    geometry = geometry_column,
                    crs = crs_value
                )
            
            except:
                gpd_input = gpd.GeoDataFrame(
                    pd_input,
                    geometry = gpd.points_from_xy(pd_input['longitude'], pd_input['latitude'], crs=crs_value)
                ).drop('location', axis=1)

                gpd_input.rename_geometry('location', inplace=True)

                return gpd_input