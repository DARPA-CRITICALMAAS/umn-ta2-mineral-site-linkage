import pickle
import polars as pl

from utils.dataframe_operations import *

def merge_grouping_results(pl_grouped1, pl_grouped2):
    # Rename groupid
    pl_grouped1 = pl_grouped1.rename(
        {'GroupID': 'GroupID1'}
    )
    pl_grouped2 = pl_grouped2.rename(
        {'GroupID': 'GroupID2'}
    )

    # Group by combination of two GroupIDs
    pl_combined = pl.concat(
        [pl_grouped1, pl_grouped2],
        how='align'
    ).group_by(
        ['GroupID1', 'GroupID2']
    ).agg([pl.all()]).drop(
        ['GroupID1', 'GroupID2']
    )

    return add_index_columns(pl_combined, 'GroupID')