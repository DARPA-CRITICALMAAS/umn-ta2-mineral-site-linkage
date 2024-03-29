import polars as pl
import pandas as pd
import geopandas as gpd
from shapely import wkt

def to_polars(df_input, input_dataframe_type: str):
    match input_dataframe_type:
        case 'pd':
            return pl.from_pandas(df_input)
        
        case 'gpd':
            pd_input = pd.DataFrame(df_input)
            return pl.from_pandas(pd_input)
        
        case _:
            return df_input

def to_pandas(df_input, input_dataframe_type: str):
    match input_dataframe_type:
        case 'pl':
            return df_input.to_pandas()
        
        case 'gpd':
            return df_input.drop('geometry', axis=1)
        
        case _:
            return df_input

def to_geopandas(df_input, input_dataframe_type: str, geometry_column: list|str):
    pd_input = to_pandas(df_input, input_dataframe_type)
    crs_value = pd_input['crs'][0]      # TODO: Check

    if isinstance(geometry_column, list):
        return gpd.GeoDataFrame(
            pd_input,
            geometry = gpd.points_from_xy(pd_input['longitude'], pd_input['latitude'], crs=crs_value)
        )
    
    else:
        pd_input['location'] = pd_input['location'].apply(wkt.loads)

        return gpd.GeoDataFrame(
            pd_input,
            geometry = 'location',
            crs = crs_value
        )