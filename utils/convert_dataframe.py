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
                return df_input.drop('location', axis=1)
            except:
                return pd.DataFrame(df_input)
        
        case _:
            return df_input

def to_geopandas(df_input, input_dataframe_type: str, geometry_column: list|str):
    pd_input = to_pandas(df_input, input_dataframe_type)
    crs_value = pd_input['crs'][0]

    try:
        pd_input[geometry_column] = gpd.GeoSeries.from_wkt(pd_input[geometry_column])

        return gpd.GeoDataFrame(
            pd_input,
            geometry = 'location',
            crs = crs_value
        )
    
    except:
        return gpd.GeoDataFrame(
            pd_input,
            geometry = gpd.points_from_xy(pd_input['longitude'], pd_input['latitude'], crs=crs_value)
        ).rename_geometry('location')