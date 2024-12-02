import os
import requests
import configparser

import pandas as pd
import polars as pl
import networkx as nx

import pyproj
from shapely import wkt, ops
from shapely.wkt import loads, dumps
from shapely.errors import WKTReadingError
import warnings

class QueryKG:
    def __init__(self,
                 endpoint:str = 'https://minmod.isi.edu/sparql',
                 crs: str='EPSG:4326') -> None:
        
        """
        Initiate KG Querying

        Arguments
        : endpoint: querying endpoint
        : crs: coordinate reference system
        """
        
        self.endpoint = endpoint
        self.crs = crs
        self.data = pl.DataFrame()

    def get_data(self,
                 commodity_code:str=None,
                 country_code:str='ALL',
                 state_code:str='ALL') -> pl.DataFrame:
        """
        Retrieves data by querying the Knowledge Graph (minmod.isi.edu)

        Arguments:
        : commodity_code:
        : country_code:
        : state_code:

        Return:

        """
        # TODO: fill self.data with the data queried from knowledge graph
        pass

    def get_sameas(self) -> pl.DataFrame:
        """
        Retrieves same_as links by querying the Knowledge Graph (minmod.isi.edu)
        """
        # TODO: get sameas links
        pass

    def safe_wkt_load(self,
                      wkt_string:str):
        try:
            return loads(wkt_string)
        except WKTReadingError as e:
            print(f"Error converting WKT: {e}")
            return None

    def geom_range_check(self,
                         input_geometry:str = None,
                         input_crs:str = None,) -> bool:
        pass

    def epsg_convert(self,
                     input_geometry:str = None,
                     input_crs:str = None,) -> str:
        """
        Convergs the crs of the geometry

        Arguments
        : input_geometry: geometry in the string format
        : input_crs: crs of the input geometry

        Return
        : str_geometry: geometry converted into EPSG 4326 (WGS84)
        """
        
        try:
            shape_geometry = wkt.loads(input_geometry)

            return input_geometry
        except:
            return -1

    def run_minmod_query(self,
                         query:str, 
                         values:bool = False):
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
            url=self.endpoint,
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
        
    