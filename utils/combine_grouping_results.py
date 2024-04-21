import pickle
import polars as pl

from utils.dataframe_operations import *

def merge_grouping_results(pl_grouped):
    possible_columns = set(['GroupID_location', 'GroupdID_text'])
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

            return add_index_columns(pl_grouped, 'GroupID')