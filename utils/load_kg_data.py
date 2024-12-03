import os
import requests
import configparser

import pandas as pd
import polars as pl
import networkx as nx

from shapely.wkt import loads
from shapely.errors import WKTReadingError
import warnings

warnings.filterwarnings("ignore")

config = configparser.ConfigParser()
config.read('./params.ini')

path_params = config['directory.paths']
minmod_params = config['minmod']

def safe_wkt_load(wkt_string):
    try:
        return loads(wkt_string)
    except WKTReadingError as e:
        print(f"Error converting WKT: {e}")
        return None
    
def run_sparql_query(query, endpoint=minmod_params['END_POINT'], values=False):
    # add prefixes
    final_query = '''
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX : <https://minmod.isi.edu/ontology/>
    PREFIX mndr: <https://minmod.isi.edu/resource/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX gkbi: <https://geokb.wikibase.cloud/entity/>
    PREFIX gkbt: <https://geokb.wikibase.cloud/prop/direct/>
    PREFIX geo: <http://www.opengis.net/ont/geosparql#>
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
    try:
        qres = response.json()
        if "results" in qres and "bindings" in qres["results"]:
            df = pd.json_normalize(qres['results']['bindings'])
            if values:
                filtered_columns = df.filter(like='.value').columns
                df = df[filtered_columns]
            return df
    except:
        return None
    
# def run_sparql_query(query, values=False):
#     return run_sparql_query(query, endpoint='https://minmod.isi.edu/sparql', values=values)

# def run_geokb_query(query, values=False):
#     return run_sparql_query(query, endpoint='https://geokb.wikibase.cloud/query/sparql', values=values)

def load_minmod_kg(commodity:str):
    pl_commodity = pl.read_csv(os.path.join(path_params['PATH_MAPFILE_DIR'], path_params['PATH_COMMODITY_MAPFILE']))
    commodity_QID = pl_commodity.filter(
        pl.col('name').str.to_lowercase() == commodity.lower()
    ).item(0, 'id')

    del pl_commodity

    # try:
    query = """
        SELECT ?ms ?source_id ?record_id ?ms_name ?aliases ?country ?state_or_province ?loc_wkt ?crs
        WHERE {
            ?ms a :MineralSite ;
                :source_id ?source_id ;
                :record_id ?record_id .
            
            OPTIONAL { ?ms rdfs:label ?ms_name . }
            OPTIONAL { ?ms skos:altLabel ?aliases . }
            OPTIONAL { 
                ?ms :location_info ?loc . 
                OPTIONAL { ?loc :country/:normalized_uri/rdfs:label ?country . }
                OPTIONAL { ?loc :state_or_province/:normalized_uri/rdfs:label ?state_or_province . }
                OPTIONAL { ?loc :location ?loc_wkt . }
                OPTIONAL { ?loc :crs/:normalized_uri/rdfs:label ?crs . }
            }

            ?ms :mineral_inventory/:commodity/:normalized_uri mndr:%s.
        }
    """ % (commodity_QID)

    pl_ms = pl.from_pandas(run_sparql_query(query, values=True))

    pl_ms = pl_ms.rename(
        {'ms.value': 'ms_uri',
        'source_id.value': 'source_id',
        'record_id.value': 'record_id',
        'ms_name.value': 'ms_name',
        'country.value': 'country',
        'state_or_province.value': 'state_or_province',
        'loc_wkt.value': 'location',
        'crs.value': 'crs'}
    )
    try:
        pl_ms = pl_ms.rename(
            {'aliases.value': 'other_names'}
        )
    except:
        pass
    
    pl_ms = pl_ms.group_by(
        'ms_uri'
    ).agg([pl.all()])

    pl_ms = pl_ms.with_columns(
        pl.exclude('ms_uri').list.unique().list.join(',')
    ).with_columns(
        pl.col('record_id').cast(pl.Utf8)
    ).drop('ms_name')


    query = """
        SELECT ?ms ?miq_comm
        WHERE {
            ?ms a :MineralSite .

            ?ms :mineral_inventory/:commodity/:normalized_uri mndr:%s.
            ?ms :mineral_inventory/:commodity/:normalized_uri/rdfs:label ?miq_comm .
        }
    """ % (commodity_QID)
    pl_comm = pl.from_pandas(run_sparql_query(query, values=True))
    pl_comm = pl_comm.rename(
        {'ms.value': 'ms_uri',
        'miq_comm.value': 'commodity'}
    ).group_by(
        'ms_uri'
    ).agg([pl.all()]).with_columns(
        pl.exclude('ms_uri').list.unique().list.join(',')
    )

    query = """
        SELECT ?ms ?deposit_type
        WHERE {
            ?ms a :MineralSite .

            ?ms :mineral_inventory/:commodity/:normalized_uri mndr:%s.

            OPTIONAL {
                ?ms :deposit_type_candidate/:observed_name ?deposit_type .
            }
        }
    """ % (commodity_QID)
    pl_dep_type = pl.from_pandas(run_sparql_query(query, values=True))
    pl_dep_type = pl_dep_type.rename(
        {'ms.value': 'ms_uri',
        'deposit_type.value': 'deposit_type'}
    ).group_by(
        'ms_uri'
    ).agg([pl.all()]).with_columns(
        pl.exclude('ms_uri').list.unique().list.sort().list.join(',')
    )

    pl_sites = pl.concat(
        [pl_ms, pl_comm, pl_dep_type],
        how='align'
    )

    return pl_sites

def separate_data(pl_sites):
    list_all_data_sources = set(pl_sites['source_id'].to_list())

    source_linked = pl_sites.filter(
        pl.col('same_as').is_not_null()
    )['source_id'].to_list()
    source_linked = set(source_linked)

    source_not_linked = list_all_data_sources - source_linked

    pl_already_linked = pl_sites.filter(
        pl.col('source_id').is_in(list(source_linked))
    )
    pl_not_linked = pl_sites.filter(
        pl.col('source_id').is_in(list(source_not_linked))
    )

    return [pl_not_linked, pl_already_linked]