import pickle
import polars as pl

from utils.dataframe_operations import *

def merge_grouping_results(pl_grouped, source_id:str, condition:str|None=None):
    possible_columns = set(['GroupID_location', 'GroupID_text'])
    columns_in_dataframe = set(list(pl_grouped.columns))

    column_overlap = list(possible_columns & columns_in_dataframe)

    match len(column_overlap):
        case 0:
            return pl_grouped
        
        case 1:
            return pl_grouped.rename(
                {column_overlap[0]: 'GroupID'}
            )
        
        case _:
            pl_grouped = pl_grouped.group_by(
                column_overlap
            ).agg([pl.all()]).drop(column_overlap)

            if condition:
                pl_grouped = pl_grouped.rename({'loc_GroupID': 'GroupID'})
                pl_multi = pl_grouped.with_columns(
                    unique_count = pl.col('link_method').list.unique().list.len(),
                    link_count = pl.col('link_method').list.len()
                ).filter(
                    pl.col('unique_count') == 2,
                    pl.col('link_count') == 2,
                ).drop(['link_count', 'unique_count'])

                pl_multi = add_index_columns(pl_multi, 'tmpID')
                pl_multi = pl_multi.with_columns(
                    GroupID = pl.lit(source_id) + pl.col('tmpID').cast(pl.Utf8)
                ).drop('tmpID')
                
                grouped_uris = pl_multi['ms_uri'].to_list()

                pl_grouped = pl_grouped.explode(list(pl_grouped.columns))

                pl_grouped = pl_grouped.with_columns(
                    filter_col = pl.col('ms_uri').is_in(grouped_uris)
                ).filter(
                    pl.col('filter_col') == False
                ).drop('filter_col')

                pl_grouped = pl.concat(
                    [pl_grouped, pl_multi],
                    how='diagonal'
                )

                return pl_grouped

            if 'GroupID' in list(pl_grouped.columns):
                pl_grouped = pl_grouped.drop(
                    'GroupID'
                )

            pl_grouped = add_index_columns(pl_grouped, 'tmpID')
            pl_grouped = pl_grouped.with_columns(
                GroupID = pl.lit(source_id) + pl.col('tmpID').cast(pl.Utf8)
            ).drop('tmpID')

            return pl_grouped