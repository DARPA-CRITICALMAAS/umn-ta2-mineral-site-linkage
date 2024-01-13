import geopandas as gpd
import polars as pl

def define_region(pl_data):
    pl_intra_data = pl_data.group_by(
        'GroupID'
    ).agg(
        [pl.all()]
    ).sort('GroupID')

    df_data = pl_data.to_pandas()

    gpd_poly = gpd.GeoDataFrame(
        df_data, geometry = gpd.points_from_xy(df_data['longitude'], df_data['latitude'], crs='WGS84')
    ).to_crs(3857)

    gpd_poly = gpd_poly.dissolve('GroupID').convex_hull.buffer(10)   # 5 meter buffer on projected map
    gpd_poly.name = 'rep_geometry'
    gpd_poly = gpd_poly.to_crs('WGS84').to_frame().reset_index().sort_values(by=['GroupID'])
    gpd_poly = gpd_poly.set_geometry('rep_geometry')

    return pl_intra_data, gpd_poly

def check_overlap():
    return 0

def select_max_overlap():
    return 0

def location_based_linking(pl_intra_linked, df_geom):
    pl_intra_linked = pl_intra_linked.sort('idx')
    df_geom = df_geom.sort('idx').drop('idx')

    pl_data = pl.concat(
        [pl_intra_linked, df_geom],
        how='horizontal'
    )

    pl_intra_data, gpd_poly = define_region(pl_data)