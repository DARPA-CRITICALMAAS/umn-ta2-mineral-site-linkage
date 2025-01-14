import requests
from typing import Dict, List

import pandas as pd
import polars as pl

from shapely import wkt
from shapely.wkt import loads
from shapely.errors import WKTReadingError

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
                 list_commodity_code:list=None,
                 country_code:str=None,
                 state_code:str=None) -> Dict[str, pl.DataFrame]:
        """
        Retrieves data by querying the Knowledge Graph (minmod.isi.edu)

        Arguments:
        : list_commodity_code:
        : country_code:
        : state_code:

        Return:

        """
        dict_data = {}

        for c in list_commodity_code:
            dict_data[c] = self.load_minmod(c)
    
        if country_code and state_code:
            # Add feature to filter by county and state code
            pass

        return dict_data

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
        
    def load_minmod(self,
                    commodity_code:str):
        """
        TODO: Fill Information

        """
        
        # TODO: Cleanup query
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
        """ % (commodity_code)

        pl_ms = pl.from_pandas(self.run_minmod_query(query, values=True))

        pl_ms = pl_ms.rename(
            {'ms.value': 'ms_uri',
            'source_id.value': 'source_id',
            'record_id.value': 'record_id',
            'ms_name.value': 'site_name',
            'country.value': 'country',
            'state_or_province.value': 'state_or_province',
            'loc_wkt.value': 'location',
            'crs.value': 'crs',}
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
        )

        query = """
            SELECT ?ms ?miq_comm
            WHERE {
                ?ms a :MineralSite .

                ?ms :mineral_inventory/:commodity/:normalized_uri mndr:%s.
                ?ms :mineral_inventory/:commodity/:normalized_uri/rdfs:label ?miq_comm .
            }
        """ % (commodity_code)
        pl_comm = pl.from_pandas(self.run_minmod_query(query, values=True))
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
        """ % (commodity_code)
        pl_dep_type = pl.from_pandas(self.run_minmod_query(query, values=True))
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
