import statistics
import polars as pl
import geopandas as gpd

def point_rep(gpd_input: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    # Convert geometry to point using centroid
    gpd_input['geometry'] = gpd_input['geometry'].apply(lambda x: x.centroid if x.type!="Point" else x)
    
    return gpd_input

def area_rep(gpd_input: gpd.GeoDataFrame) -> gpd.GeoDataFrame: 
    # Apply buffer to convert input polygon
    gpd_input['geometry'] = gpd_input['geometry'].buffer(distance=3000)

    return gpd_input










# def create_coordinate_point_representation(gpd_data):
#     # Convert geometry to single point by finding centroid if the geometry is not a point
#     gpd_data['location'] = gpd_data['location'].apply(lambda x: x.centroid if x.type!='Point' else x)

#     # Create a longitude, latitude column
#     gpd_data['longitude'] = gpd_data.location.x
#     gpd_data['latitude'] = gpd_data.location.y

#     pl_data = to_polars(gpd_data, 'gpd')

#     # aggregate the data based on the GroupID (create a list of latitudes and longitudes under the same group)
#     pl_data = pl_data.group_by(
#         'GroupID'
#     ).agg(
#         [pl.all()]
#     )

#     # Find the mean value of the latitudes and longitudes under the same group
#     pl_data = pl_data.with_columns(
#         pl.col('latitude').list.mean(),
#         pl.col('longitude').list.mean(),
#     )
#     pl_data = pl_data.with_columns(
#         pl.exclude(['latitude', 'longitude', 'GroupID']).list.unique().list.join(separator=',')
#     )

#     pl_data = pl_data.drop('GroupID')

#     # convert back to geodataframe using the average longitude and latitude value
#     gpd_data = to_geopandas(pl_data, 'pl', ['longitude', 'latitude'])

#     return gpd_data

# def create_buffer_area_representation(gpd_data):
#     # Convert to metric system
#     gpd_data = gpd_data.to_crs(crs=geo_params['METRIC_CRS_SYSTEM'])
    
#     # Dissolving based on GroupID and creating an integer convex hull around it
#     gpd_polygon = gpd_data.dissolve(
#         'GroupID'
#     ).convex_hull.buffer(
#         float(geo_params['POLYGON_BUFFER_UNIT_METER'])
#     )

#     gpd_polygon = gpd_polygon.to_frame().reset_index().sort_values(by=['GroupID']).rename_geometry('location')

#     pl_data = to_polars(gpd_data, 'gpd').group_by(
#         'GroupID'
#     ).agg([pl.all()])

#     pl_data = pl_data.with_columns(
#         pl.exclude(['latitude', 'longitude', 'GroupID']).list.unique().list.join(separator=',')
#     )

#     pd_data = to_pandas(pl_data, 'pl').drop('location', axis=1)
#     gpd_polygon = gpd_polygon.to_crs(crs=geo_params['DEFAULT_CRS_SYSTEM'])
#     gpd_polygon = gpd_polygon.merge(pd_data, on='GroupID')

#     return gpd_polygon