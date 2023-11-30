import geopandas as gpd

def unify_gdb_crs(gdf):
    gdf['geometry'] = gdf['geometry'].to_crs(crs='WGS84')

    return gdf

def convert_to_gdb(df_data, col_longitude, col_latitude, crs='WGS84'):
    gdf = gpd.GeoDataFrame(
        df_data, geometry=gpd.points_from_xy(df_data[col_longitude], df_data[col_latitude], crs=crs)
    )

    if gdf.crs != 'WGS84':
        gdf = unify_gdb_crs(gdf)

    return gdf