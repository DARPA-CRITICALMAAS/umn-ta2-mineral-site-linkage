import polars as pl
from itertools import product

def save_intermediate(
    fusemine_model,
    mode: str='csv',
):
    """
    Save intermediate data

    Arguments:
        - fusemine_model
        - mode

    Returns:

    Examples:
    ```
    fusemine_model.save_intermediate(mode='CSV')
    ```
    """

    return 0

def save_sameas(pl_data, col_group:str):
    # input pl_data: (unique_col, col_group)
    pl_data = pl_data.groupby(col_group).agg([pl.all()])

    return 0