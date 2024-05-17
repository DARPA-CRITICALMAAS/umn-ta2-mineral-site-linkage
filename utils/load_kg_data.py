import requests

import pandas as pd
import polars as pl
import networkx as nx

from shapely.wkt import loads
from shapely.errors import WKTReadingError
import warnings

warnings.filterwarnings("ignore")

def safe_wkt_load(wkt_string):
    try:
        return loads(wkt_string)
    except WKTReadingError as e:
        print(f"Error converting WKT: {e}")
        return None
    
def run_sparql_query(query, endpoint='https://minmod.isi.edu/sparql', values=False):
    # add prefixes
    final_query = '''
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX : <https://minmod.isi.edu/resource/>
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
    
def run_minmod_query(query, values=False):
    return run_sparql_query(query, endpoint='https://minmod.isi.edu/sparql', values=values)

def run_geokb_query(query, values=False):
    return run_sparql_query(query, endpoint='https://geokb.wikibase.cloud/query/sparql', values=values)

def minmod_kg_check(source_name:str|None=None, record_id:str|None=None):
    # Check if record id exists
    if record_id:
        query = '''
        SELECT ?ms
        WHERE {
            ?ms rdfs:label|:record_id ?record_id . FILTER(STR(?record_id) == "%s")
        }
        ''' % (source_name)
        query_resp_df = run_minmod_query(query, values=True)
        
        return query_resp_df

    if source_name:
        query = '''
        SELECT ?ms ?source_id ?record_id ?ms_name ?country ?state_or_province ?loc_wkt ?crs ?miq_comm ?deposit_type
        WHERE {
            ?ms a :MineralSite .

            ?ms :source_id ?source_id . FILTER (STR(?source_id) = "%s")
            ?ms :record_id ?record_id . FILTER (STR(?record_id) != "")

            OPTIONAL { ?ms :name ?ms_name . }

            OPTIONAL { ?ms :location_info ?loc . 
                OPTIONAL { ?loc :country ?country }
                OPTIONAL { ?loc :state_or_province ?state_or_province }
                OPTIONAL { ?loc :location ?loc_wkt }
                OPTIONAL { ?loc :crs ?crs }
            }

            ?ms  :mineral_inventory ?miq .
            ?miq :commodity/:name   ?miq_comm .

            OPTIONAL { ?ms :deposit_type_candidate ?md .
                OPTIONAL { ?md :observed_name ?deposit_type }
            }
        }
        ''' % (source_name)
        query_resp_df = run_minmod_query(query, values=True)

        if not query_resp_df.empty:
            sites_df = pd.DataFrame([
                {
                    'ms_uri': row['ms.value'],
                    'source_id':row['source_id.value'],
                    'record_id':row['record_id.value'],
                    'site_name': row['ms_name.value'] if len(str(row['ms_name.value'])) > 0 else row['ms.value'].split('/')[-1],
                    'country': row.get('country.value', None),
                    'state_or_province': row.get('state_or_province.value', None),
                    'location': row.get('loc_wkt.value', None),
                    'crs': row.get('crs.value', None),
                    'commodity': row.get('miq_comm.value', None),
                    'deposit_type': row.get('deposit_type.value', None)
                }
                for index, row in query_resp_df.iterrows()
            ])

            pl_sites = pl.from_pandas(sites_df).group_by(
                'ms_uri'
            ).agg([pl.all()]).with_columns(
                pl.exclude('ms_uri', 'location').list.unique().list.join(',')
            ).with_columns(
                pl.col('record_id').cast(pl.Utf8),
                pl.col('location').list.get(0)
            )
        
        return pl_sites


def load_minmod_kg(commodity:str):
    # commodity = args.commodity
    # query = ''' SELECT ?ms1 ?ms2
    #             WHERE {
    #                 ?ms1 a :MineralSite .
    #                 ?ms2 a :MineralSite .
    
    #                 ?ms1 owl:sameAs ?ms2 .
    #             } '''
    # df_all_sites = run_minmod_query(query, values=True)
    # G = nx.from_pandas_edgelist(df_all_sites, source='ms1.value', target='ms2.value')

    # # Find connected components
    # groups = nx.connected_components(G)

    # # Create a mapping from value to group ID
    # group_mapping = {}
    # for group_id, group in enumerate(groups, start=1):
    #     for value in group:
    #         group_mapping[value] = group_id

    # # Map group IDs to the dataframe
    # df_all_sites['group_id'] = df_all_sites['ms1.value'].map(group_mapping)

    # ------------------ Hyper Site (aggregated group of sites) to Mineral Site ------------------

    # get all Mineral Sites
    # query = '''
    # SELECT ?ms ?source_id ?record_id ?ms_name
    # WHERE {

    #     ?ms :mineral_inventory ?mi .
    #     ?ms :deposit_type_candidate ?md .
    #     OPTIONAL { ?ms rdfs:label|:name ?ms_name . FILTER (STR(?ms_name) != "") }
    #     OPTIONAL { ?ms rdfs:label|:source_id ?source_id . FILTER(LCASE(STR(?source_id)) = "mrds") }
    #     OPTIONAL { ?ms rdfs:label|:record_id ?record_id . FILTER (STR(?record_id) != "") }
    # }
    # '''

    # query_resp_df = run_minmod_query(query, values=True)

    # pl_data = pl.from_pandas(query_resp_df).filter(
    #     pl.col('record_id.value') == '10100737'
    # )
    # print(pl_data)
    # pl_data = pl.from_pandas(query_resp_df).filter(
    #     pl.col('record_id.value') == '10100737'
    # )

    # Get commodity specific Mineral Sites
    # query = '''
    # SELECT ?ms ?source_id ?record_id ?ms_name ?country ?loc_wkt ?crs ?state_or_province ?commod ?miq_comm

    # WHERE {
    #     ?ms a :MineralSite .
    #     ?ms :mineral_inventory ?mi .
    #     ?ms :deposit_type_candidate ?md .

    #     OPTIONAL { ?ms :name ?ms_name }
    #     OPTIONAL { ?ms :source_id ?source_id }
    #     OPTIONAL { ?ms :record_id ?record_id }

    #     ?mi :observed_commodity ?commod . FILTER(LCASE(STR(?commod)) = "%s")
    # }

    # ''' % (commodity)

    query = ''' SELECT ?ms ?source_id ?record_id ?ms_name ?country ?state_or_province ?loc_wkt ?crs ?miq_comm ?deposit_type
        WHERE {
            ?ms a :MineralSite .
            ?ms :mineral_inventory ?mi .

            ?ms :source_id ?source_id .
            ?ms :record_id ?record_id .
            OPTIONAL { ?ms :name ?ms_name . }

            OPTIONAL { ?ms :location_info ?loc . 
                OPTIONAL { ?loc :country ?country }
                OPTIONAL { ?loc :state_or_province ?state_or_province }
                OPTIONAL { ?loc :location ?loc_wkt }
                OPTIONAL { ?loc :crs ?crs }
            }

            ?mi :commodity [ :name ?commod ] . FILTER(LCASE(STR(?commod)) = "%s")

            ?ms  :mineral_inventory ?miq .
            ?miq :commodity/:name   ?miq_comm .

            OPTIONAL { ?ms :deposit_type_candidate ?md .
                OPTIONAL { ?md :observed_name ?deposit_type }
            }
        }
        ''' % (commodity)

    # OPTIONAL { ?ms :location_info ?loc . 
    #             OPTIONAL { ?loc :country ?country }
    #             OPTIONAL { ?loc :state_or_province ?state_or_province }
    #             OPTIONAL { ?loc :location ?loc_wkt }
    #             OPTIONAL { ?loc :crs ?crs }
    #         }
            #     OPTIONAL { ?ms  :mineral_inventory ?miq .
            #     OPTIONAL { ?miq :commodity/:name   ?miq_comm }
            # }

    query_resp_df = run_minmod_query(query, values=True)

    if not query_resp_df.empty:
        sites_df = pd.DataFrame([
            {
                'ms_uri': row['ms.value'],
                'source_id':row['source_id.value'],
                'record_id':row['record_id.value'],
                'site_name': row['ms_name.value'] if len(str(row['ms_name.value'])) > 0 else row['ms.value'].split('/')[-1],
                'country': row.get('country.value', None),
                'state_or_province': row.get('state_or_province.value', None),
                'location': row.get('loc_wkt.value', None),
                'crs': row.get('crs.value', None),
                'commodity': row.get('miq_comm.value', None),
                'deposit_type': row.get('deposit_type.value', None)
            }
            for index, row in query_resp_df.iterrows()
        ])

        pl_sites = pl.from_pandas(sites_df).group_by(
            'ms_uri'
        ).agg([pl.all()]).with_columns(
            pl.exclude('ms_uri').list.unique().list.join(',')
        ).with_columns(
            pl.col('record_id').cast(pl.Utf8)
        )

        # ------------ GENERATES HYPERSITES ------------ #
        # sites_df.dropna(subset=['country', 'state_or_province', 'loc_wkt'], how='all', inplace=True)

        # df_melted = df_all_sites.melt(id_vars=['group_id'], value_vars=['ms1.value', 'ms2.value'], value_name='ms')

        # df_all_sites_groups = df_melted[['ms', 'group_id']].drop_duplicates()
        # merged_df_all_sites = pd.merge(sites_df, df_all_sites_groups, how='left', on='ms')

        # max_group_id = merged_df_all_sites['group_id'].fillna(0).max()
        # merged_df_all_sites['group_id'] = merged_df_all_sites['group_id'].fillna(pd.Series(range(int(max_group_id) + 1, len(merged_df_all_sites) + int(max_group_id) + 1)))
        # sorted_df_all_sites_all_dep = merged_df_all_sites.sort_values(by=['group_id', 'ms_name'])

        # sorted_df_all_sites_all_dep.reset_index(drop=True, inplace=True)
        # sorted_df_all_sites_all_dep.set_index('ms', inplace=True)
        # sorted_df_all_sites_all_dep['info_count'] = sorted_df_all_sites_all_dep[['country', 'state_or_province', 'loc_wkt']].apply(lambda x: ((x != '') & (x.notna())).sum(), axis=1)
        # sorted_df_all_sites_all_dep = sorted_df_all_sites_all_dep.sort_values(by='info_count', ascending=False)
        # sorted_df_all_sites_all_dep = sorted_df_all_sites_all_dep[~sorted_df_all_sites_all_dep.index.duplicated(keep='first')]
        # sorted_df_all_sites_all_dep.drop(columns=['info_count'], inplace=True)
        # sorted_df_all_sites_all_dep.reset_index(inplace=True)

        # sorted_df_all_sites_all_dep.to_csv(f'{output_directory}/{commodity}_mineral_sites_hypersites.csv', index=False, mode='w')

        return pl_sites
        # return separate_data(pl_sites)
    
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