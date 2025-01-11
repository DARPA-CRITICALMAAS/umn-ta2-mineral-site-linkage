import os
import polars as pl

def load_df(path_file:str, file_type:str) -> pl.DataFrame:
    """
    TODO: Fill information

    Arguments:
    path_file:
    file_type:
    """
    pl_data = pl.read_csv(f"{path_file}.{file_type}")

    return pl_data

def check_mode(path_file: str) -> str:
    _, file_extension = os.path.splitext(path_file)

    if file_extension:
        return file_extension
    
    return 'dir'

def combine_dfs(dfgt: pl.DataFrame,
                dfpred: pl.DataFrame) -> pl.DataFrame:
    # Preserve only those that are available on the ground truth dataset
    dfgt = dfgt.join(dfpred, left_on=['ms_uri_1', 'ms_uri_2'])

    return dfgt