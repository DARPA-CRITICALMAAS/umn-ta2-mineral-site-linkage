import numpy as np
import polars as pl
import pandas as pd
import geopandas as gpd
from sklearn.cluster import HDBSCAN

def location_based_linking(pl_geom, bool_location):
    if bool_location:
        epsilon = 0.005
    else:
        epsilon = 0.05

    coords = pl_geom[['longitude', 'latitude']].to_numpy()
    clusters = HDBSCAN(min_cluster_size=2, cluster_selection_epsilon=epsilon).fit(coords)

    pl_loc_linked = pl_geom.with_columns(
        GroupID = pl.Series(clusters.labels_),
    )

    return pl_loc_linked

def define_region(pl_data, crs_code):
    pl_individual = pl_data.filter(
        pl.col('GroupID') == -1
    ).drop(
        'GroupID'
    ).with_columns(
        GroupID = pl.Series(list(range(pl_data.shape[0], pl_data.shape[0] + pl_individual.shape[0])))
    )

    print(pl_individual.columns)

    pl_data = pl_data.filter(
        pl.col('GroupID') != -1
    )
    pl_intra_data = pl_data.group_by(
        'GroupID'
    ).agg(
        [pl.all()]
    ).sort('GroupID')

    df_data = pl_data.to_pandas()

    gpd_poly = gpd.GeoDataFrame(
        df_data, geometry = gpd.points_from_xy(df_data['longitude'], df_data['latitude'], crs=crs_code)
    ).to_crs(3857)

    gpd_poly = gpd_poly.dissolve('GroupID').convex_hull.buffer(10)   # 5 meter buffer on projected map
    gpd_poly.name = 'rep_geometry'
    gpd_poly = gpd_poly.to_crs('WGS84').to_frame().reset_index().sort_values(by=['GroupID'])
    gpd_poly = gpd_poly.set_geometry('rep_geometry')

    return pl_intra_data, gpd_poly 

def create_geom_data(pl_data, columns):
    long = columns[0]
    lati = columns[1]
    crs = columns[2]

    source_id = columns[3]
    record_id = columns[4]

    pl_geom = pl_data.select(
        source_id = pl.lit(source_id),
        record_id = pl.col(record_id),
        longitude = pl.col(long),
        latitude = pl.col(lati),
        crs = pl.lit(crs)
    )

    return pl_geom

def find_max_area_group(struct: dict) -> int:
    max_area = max(struct['area'])
    index_max_area = struct['area'].index(max_area)
    selected_grouping = struct['GroupID_2'][index_max_area]

    return selected_grouping

def find_overlapping_region(gpd_poly1, gpd_poly2):
    gpd_overlapped = gpd.overlay(gpd_poly1, gpd_poly2, how='intersection')
    gpd_overlapped = gpd_overlapped.to_crs({'proj': 'cea'}) # cylidrical equal area format preserve are measure

    gpd_overlapped['area'] = gpd_overlapped.area / 10**6
    df_overlapped = pd.DataFrame(gpd_overlapped).drop('geometry', axis=1)
    pl_overlapped = pl.from_pandas(df_overlapped)

    pl_overlapped_g1 = pl_overlapped.group_by(
        'GroupID_1'
    ).agg(
        [pl.all()]
    ).with_columns(
        count = pl.col('GroupID_2').list.len()
    ).filter(

        pl.col('count') > 1
    ).with_columns(
        selected=pl.struct(['GroupID_2', 'area']).map_elements(find_max_area_group)
    )
    # pl! 

    # df.select(pl.col("values").explode())

    # pl_overlapped_g1 = pl_overlapped.partition_by(
    #     by='GroupID_1',
    #     as_dict=True
    # )

    print(pl_overlapped_g1)

    # pl_overlapped_g2 = pl_overlapped.group_by(
    #     'GroupID_2'
    # ).agg(
    #     [pl.all()]
    # ).with_columns(
    #     count = pl.col('GroupID_1').list.len()
    # ).filter(
    #     pl.col('count') > 1
    # )

    # print(pl_overlapped_g2)

    # gpd_test1 = gpd_overlapped.groupby('GroupID_1').agg(lambda x: list(x))
    # gpd_test1['count'] = gpd_test1['GroupID_2'].apply(lambda x: len(x))
    # gpd_test1_multi = gpd_test1[gpd_test1['count'] > 1]

    # gpd_test2 = gpd_overlapped.groupby('GroupID_2').agg(lambda x: list(x))
    # gpd_test2['count'] = gpd_test2['GroupID_1'].apply(lambda x: len(x))
    # gpd_test2_multi = gpd_test2[gpd_test2['count'] > 1]

    # print(gpd_test1_multi)
    # print(gpd_test2_multi)

    return gpd_overlapped

mrds_zinc = '/home/yaoyi/pyo00005/CriticalMAAS/src/data/MVTZinc/MRDS.csv'
alaska_zinc = '/home/yaoyi/pyo00005/CriticalMAAS/src/data/MVTZinc/10.5066_P96MMRFD.csv'

mrds_data = pl.read_csv(mrds_zinc)
alaska_data = pl.read_csv(alaska_zinc)

mrds_geom = create_geom_data(mrds_data, ['longitude', 'latitude', 'WGS84', 'MRDS', 'dep_id'])
alaska_geom = create_geom_data(alaska_data, ['Longitude', 'Latitude', 'NAD27', '10.5066/P96MMRFD', 'ARDF_no'])

mrds_geom = mrds_geom.filter(
    ~pl.all_horizontal(pl.col('longitude').is_null()),
    ~pl.all_horizontal(pl.col('latitude').is_null())
)
alaska_geom = alaska_geom.filter(
    ~pl.all_horizontal(pl.col('longitude').is_null()),
    ~pl.all_horizontal(pl.col('latitude').is_null())
)

mrds_linked = location_based_linking(mrds_geom, True)
alaska_linked = location_based_linking(alaska_geom, True)

intra_mrds, gpd_poly_mrds = define_region(mrds_linked, 'WGS84')
intra_alaska, gpd_poly_alaska = define_region(alaska_linked, 'NAD27')

gpd_overlap = find_overlapping_region(gpd_poly_mrds, gpd_poly_alaska)

# print(gpd_overlap)