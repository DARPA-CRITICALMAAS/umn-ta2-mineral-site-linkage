import geopandas as gpd
import polars as pl
import pandas as pd

from fusemine.params import *

def define_region(pl_data, alias_code, crs_code='WGS84'):
    pl_individual = pl_data.filter(
        pl.col('GroupID') == -1
    )
    length_individual = pl_individual.shape[0]
    pl_individual = pl_individual.drop(
        'GroupID'
    ).with_columns(
        GroupID = pl.Series(list(range(pl_data.shape[0], pl_data.shape[0] + length_individual)))
    ).select(
        idx = pl.col('idx'),
        latitude = pl.col('latitude'),
        longitude = pl.col('longitude'),
        GroupID = pl.col('GroupID').cast(pl.Int64)
    )

    pl_data = pl_data.filter(
        pl.col('GroupID') != -1
    ).select(
        idx = pl.col('idx'),
        latitude = pl.col('latitude'),
        longitude = pl.col('longitude'),
        GroupID = pl.col('GroupID').cast(pl.Int64)
    )

    pl_data = pl.concat(
        [pl_data, pl_individual],
        how='vertical'
    ).with_columns(
        GroupAlias = pl.lit(alias_code) + '_g_' + pl.col('GroupID').cast(pl.Utf8),
    )

    pl_intra_data = pl_data.group_by(
        'GroupID'
    ).agg(
        [pl.all()]
    ).sort(
        'GroupID'
    )

    df_data = pl_data.to_pandas()

    gpd_poly = gpd.GeoDataFrame(
        df_data, geometry = gpd.points_from_xy(df_data['longitude'], df_data['latitude'], crs=crs_code)
    ).to_crs(epsg=3857)

    gpd_poly = gpd_poly.dissolve('GroupAlias').convex_hull.buffer(INTERLINK_BUFFER)   # 15 meter buffer on projected map
    gpd_poly.name = 'rep_geometry'
    gpd_poly = gpd_poly.to_crs('WGS84').to_frame().reset_index().sort_values(by=['GroupAlias'])
    gpd_poly = gpd_poly.set_geometry('rep_geometry')

    return pl_intra_data, gpd_poly 

def find_max_area_group(struct: dict) -> int:
    selecting_col = list(struct.keys())[0]
    
    max_area = max(struct['area'])
    index_max_area = struct['area'].index(max_area)
    selected_grouping = struct[selecting_col][index_max_area]

    return selected_grouping

def find_overlapping_region(gpd_poly1, gpd_poly2, base_col, selecting_col):
    gpd_overlapped = gpd.overlay(gpd_poly1, gpd_poly2, how='intersection', keep_geom_type=False)

    if gpd_overlapped.shape[0] != 0:
        gpd_overlapped = gpd_overlapped.to_crs({'proj': 'cea'}) # cylidrical equal area format preserve area measure

        gpd_overlapped['area'] = gpd_overlapped.area

        df_overlapped = pd.DataFrame(gpd_overlapped).drop('geometry', axis=1)
        pl_overlapped = pl.from_pandas(df_overlapped)

        pl_overlapped_threshold_area = pl_overlapped.filter(
            pl.col('area') > INTERLINK_OVERLAP
        )

        pl_overlapped_threshold_area = pl_overlapped

        if(pl_overlapped_threshold_area.shape[0] != 0):
            pl_overlapped_gen = pl_overlapped_threshold_area.group_by(
                base_col
            ).agg(
                [pl.all()]
            ).with_columns(
                count = pl.col(selecting_col).list.len()
            )

            # print(pl_overlapped_gen)

            pl_overlapped_max = pl_overlapped_gen.filter(
                pl.col('count') > 1
            )

            len_overlapped_max = pl_overlapped_max.shape[0]

            pl_overlapped_one = pl_overlapped_gen.filter(
                pl.col('count') < 1.5
            ).with_columns(
                selected=pl.col(selecting_col).list.first()
            )
            
            if len_overlapped_max != 0:
                pl_overlapped_max = pl_overlapped_max.with_columns(
                    selected=pl.struct([selecting_col, 'area']).map_elements(find_max_area_group)
                )

                pl_overlapped_max = pl.concat(
                    [pl_overlapped_max, pl_overlapped_one],
                    how='vertical'
                )

                # print("columns", pl_overlapped_max.columns)
            
            else:
                pl_overlapped_max = pl_overlapped_one

            return pl_overlapped_max
    
    return -1

def create_polygon_region(pl_intra_linked, df_geom, alias_code):
    pl_intra_linked = pl_intra_linked.sort('idx')
    df_geom = df_geom.sort('idx').drop('idx')

    pl_data = pl.concat(
        [pl_intra_linked, df_geom],
        how='horizontal'
    )

    pl_intra_data, gpd_poly = define_region(pl_data, alias_code)

    return pl_intra_data, gpd_poly

def location_based_linking(seed_data, seed_poly, against_data, against_poly):
    if seed_data.shape[0] >= against_data.shape[0]:
        base_col = 'GroupAlias_2'
        selecting_col = 'GroupAlias_1'
        seed_decision = 'selected'
        against_decision = base_col
    elif seed_data.shape[0] < against_data.shape[0]:
        base_col = 'GroupAlias_1'
        selecting_col = 'GroupAlias_2'
        seed_decision = base_col
        against_decision = 'selected'

    pl_overlapped_max = find_overlapping_region(seed_poly, against_poly, base_col, selecting_col)

    pl_overlapped_max = pl_overlapped_max.with_columns(
        tmpGroup = pl.Series(list(range(pl_overlapped_max.shape[0])))
    ).unique(subset=["selected"], maintain_order=True, keep="first") #TODO: cross determine

    print("overlapping", pl_overlapped_max)

    seed_alias = pl_overlapped_max.select(
        pl.col('tmpGroup'),
        GroupID = pl.col(seed_decision).str.split('_').list.get(2).cast(pl.Int64),
    ).sort(
        by='GroupID'
    )

    against_alias = pl_overlapped_max.select(
        pl.col('tmpGroup'),
        GroupID = pl.col(against_decision).str.split('_').list.get(2).cast(pl.Int64),
    ).sort(
        by='GroupID'
    )

    seed_group = seed_alias['GroupID'].to_list()
    against_group = against_alias['GroupID'].to_list()

    grouped_seed_data = seed_data.filter(
        pl.col('GroupID').is_in(seed_group)
    ).sort('GroupID')
    indiv_seed_data = seed_data.filter(
        ~pl.col('GroupID').is_in(seed_group)
    )

    grouped_against_data = against_data.filter(
        pl.col('GroupID').is_in(against_group)
    ).sort('GroupID')
    indiv_against_data = against_data.filter(
        ~pl.col('GroupID').is_in(against_group)
    )

    all_indiv = pl.concat(
        [indiv_seed_data, indiv_against_data],
        how='vertical'
    ).select(
        pl.col(['idx', 'latitude', 'longitude'])
    )

    print("seed_data", grouped_seed_data)
    print("temporary group", seed_alias['tmpGroup'])

    grouped_seed_data = grouped_seed_data.with_columns(
        tmpGroup = seed_alias['tmpGroup']
    )

    grouped_against_data = grouped_against_data.with_columns(
        tmpGroup = against_alias['tmpGroup']
    )

    all_grouped = pl.concat(
        [grouped_seed_data, grouped_against_data],
        how='vertical'
    ).group_by(
        'tmpGroup'
    ).agg(
        pl.all().explode()
    ).select(
        pl.col(['idx', 'latitude', 'longitude'])
    )

    new_full = pl.concat(
        [all_grouped, all_indiv],
        how='vertical'
    )

    length_full = new_full.shape[0]

    tmp_data = new_full.with_columns(
        GroupID = pl.Series(list(range(0, length_full)))
    ).explode(
        ['idx', 'latitude', 'longitude']
    )

    return define_region(tmp_data, 'seed')