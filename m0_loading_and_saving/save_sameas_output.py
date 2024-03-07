import os
import pickle
import geopandas as gpd
import polars as pl

def save_sameas_output_csv(pl_linked_records, path_directory:str, file_name:str):
    """
    Saves the sameas data as a CSV file.
    CSV file is a two column URIs data where each row can be thought as a bidirectional edge in a graph

    : param: pl_linked_records = polars dataframe of the mineralsite records with the GroupID field
    : param: path_directory = 
    : param: file_name = 
    """
    if not os.path.exists(path_directory):
        os.makedirs(path_directory)

    pl_linked_records = pl_linked_records.filter(
        pl.col('GroupID') != -1         # Removing mineralsite records that do not have any link
    ).select(
        pl.col(['URI', 'GroupID'])      # Preserving the URI column and GroupID column
    ).group_by(
        'GroupID'
    ).agg(
        [pl.all()]
    )

    # Create a list where each item is a list is a cluster
    list_record_clusters = pl_linked_records['URI'].to_list()

    pl_sameas = pl.DataFrame(
        {
            'URI1': list_record_clusters
        }
    ).with_columns(
        URI2 = pl.col('URI1').list.get(0)
    ).explode(
        'URI1'
    ).filter(
        pl.col('URI1') != pl.col('URI2')
    )

    pl_sameas.write_csv(os.path.join(path_directory, file_name+'.csv'))