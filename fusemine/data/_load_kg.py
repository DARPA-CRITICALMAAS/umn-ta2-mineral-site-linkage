import os
import requests
import configparser

import pandas as pd
import polars as pl
import networkx as nx

from shapely.wkt import loads
from shapely.errors import WKTReadingError
import warnings

def get_kgdata(
    fusemine_model, 
    commodity_code: str,
    country_code: str,
    state_code: str,
):
    """
    Retrieves data by querying the Knowledge Graph (minmod.isi.edu)

    Arguments:

    Returns:

    Examples:
    ```
    fusemine_model.get_kgdata(<commodity_code>)
    ```
    """
    return 0