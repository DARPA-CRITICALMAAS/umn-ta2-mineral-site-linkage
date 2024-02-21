import os
import shutil
import json
import requests
from time import perf_counter

from tqdm import tqdm
import regex as re
import pandas as pd
import polars as pl
import geopandas as gpd
import pickle5 as pickle

import warnings
warnings.filterwarnings("ignore")
tqdm.pandas()

def run_sparql_query(query, endpoint='https://minmod.isi.edu/sparql', values=False):
    # add prefixes
    final_query = '''
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX : <https://minmod.isi.edu/resource/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX gkbi: <https://geokb.wikibase.cloud/entity/>
    PREFIX gkbp: <https://geokb.wikibase.cloud/wiki/Property:>
    PREFIX gkbt: <https://geokb.wikibase.cloud/prop/direct/>
    \n''' + query
    # send query
    response = requests.post(
        url=endpoint,
        data={'query': final_query},
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/sparql-results+json"  # Requesting JSON format
        },
        verify=False  # Set to False to bypass SSL verification as per the '-k' in curl
    )
    #print(response.text)
    try:
        qres = response.json()
        if "results" in qres and "bindings" in qres["results"]:
            df = pd.json_normalize(qres['results']['bindings'])
            if values:
                filtered_columns = df.filter(like='.value').columns
                rename_column = [i.split('.value')[0] for i in filtered_columns]

                df = df[filtered_columns]

                dict_name_map = dict(zip(filtered_columns, rename_column))

                pl_data = pl.from_pandas(df).rename(
                    dict_name_map
                )

            return pl_data
    except:
        return None
    
def run_minmod_query(query, values=False):
    return run_sparql_query(query, endpoint='https://minmod.isi.edu/sparql', values=values)

def load_kg():
    query = ''' SELECT ?ms ?source_id ?record_id ?commodity ?name ?location ?crs ?country ?state_province
            WHERE {
                ?ms a :MineralSite .
                ?ms :name ?name .
                ?ms :mineral_inventory [ :commodity [ :name "Nickel"@en ] ] .
                ?ms :mineral_inventory [ :commodity [ :name ?commodity ] ] .
                ?ms :source_id ?source_id .
                ?ms :record_id ?record_id . 
                ?ms :location_info [ :location ?location ] .
                OPTIONAL { ?ms :location_info [ :crs ?crs ] }.
                ?ms :location_info [ :country ?country ] .
                OPTIONAL { ?ms :location_info [ :state_or_province ?state_or_province ] }.
            } '''

    pl_data = run_minmod_query(query, values=True)

    pl_data = pl_data.group_by(
        'ms'
    ).agg(
        [pl.all()]
    ).drop(
        'ms'
    ).select(
        pl.all().list.unique().cast(pl.List(pl.Utf8)).list.join(", ") 
    )

    length = pl_data.shape[0]
    complete_list = range(1, length+1)

    mss_total = pl_data.with_columns(
        idx = 'mss_' + pl.Series(complete_list).cast(pl.Utf8),
        crs = pl.when(
            pl.col('crs') == 'null'
        ).then(
            pl.lit('WGS84')
        ).otherwise(
            pl.col('crs')
        )
    )