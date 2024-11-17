import requests
import polars as pl
import geopandas as gpd

def crs2crs(
    fusemine_model,
    data: pl.DataFrame, 
    input_crs: str,
    output_crs: str,
):
    """
    Converts CRS from one type to another
    """

    return 0