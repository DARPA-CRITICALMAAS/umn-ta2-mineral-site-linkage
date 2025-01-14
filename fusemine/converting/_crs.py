# import requests
import polars as pl
import geopandas as gpd
from pyproj import CRS, Transformer
from shapely import wkt, geometry, LineString

def crs2crs(input_data: pl.DataFrame, 
            input_crs: str,
            output_crs: str='EPSG:4326',
            geometry_col: str='location'):
    """
    Converts CRS from one type to another

    Argument
    : fusemine_model: 
    : data: 
    : input_crs: 
    : output_crs: 
    
    Return
    : gpd_data
    : pl_data_nl
    """
    # Initiate blank dataframes
    gpd_data = gpd.GeoDataFrame()
    pl_data_nl = pl.DataFrame()

    # Infer as output crs if input crs is missing
    if input_crs == '':
        input_data = input_data.drop('crs').with_columns(crs=pl.lit(output_crs))
        input_crs = output_crs

    # Separate to data without location and with location
    pl_data_nl = input_data.filter(pl.col(geometry_col) == '')
    input_data = input_data.filter(pl.col(geometry_col) != '').to_pandas()

    # WKT load location and check coordinate range
    input_data[geometry_col] = input_data[geometry_col].apply(lambda x: load_geom(str_geom=x, crs_val=input_crs))

    # Filter out those that are not possible geometries or out of indicated CRS bounds
    df_unloaded = input_data[input_data[geometry_col].isnull()]
    pl_unloaded = pl.from_pandas(df_unloaded)
    pl_data_nl = pl.concat(
        [pl_data_nl, pl_unloaded],
        how='diagonal'
    )

    # Convert crs to output crs for the ones with geometry loaded
    input_data = input_data[~input_data[geometry_col].isnull()]
    gpd_data = gpd.GeoDataFrame(
        input_data, 
        geometry=geometry_col,
        crs=input_crs
    ).to_crs(output_crs)

    return gpd_data, pl_data_nl

def load_geom(str_geom:str,
              crs_val:str=None):
    """
    
    """
    crs = CRS(crs_val)
    transformer = Transformer.from_crs(crs.geodetic_crs, crs, always_xy=True)

    try:
        wkt_geom = wkt.loads(str_geom)

    except:
        return None
    
    geom_type = geometry.mapping(wkt_geom)['type']
    geom_coords = list(geometry.mapping(wkt_geom)['coordinates'])

    if geom_type.upper() == 'POINT':
        long_val, lat_val = geom_coords

        if check_coodinate_range(crs=crs, transformer=transformer, lat_val=lat_val, long_val=long_val):
            return wkt_geom
        return None
    
    if geom_type.upper() == 'POLYGON':
        geom_coords = list(geom_coords[0])

    bool_within_range = True
    for long_val, lat_val in geom_coords:
        bool_within_range = bool_within_range & check_coodinate_range(crs=crs, transformer=transformer, lat_val=lat_val, long_val=long_val)
        
        if not bool_within_range:
            return None
        
    return wkt_geom

def check_coodinate_range(crs, transformer, lat_val:float, long_val:float) -> bool:
    min_long, min_lat, max_long, max_lat = transformer.transform_bounds(*crs.area_of_use.bounds)

    if (min_long <= long_val <= max_long) & (min_lat <= lat_val <= max_lat):
        return True

    return False