import os
import warnings
import urllib3
import pickle

import polars as pl
import pandas as pd

from datetime import datetime, timezone

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=pl.MapWithoutReturnDtypeWarning)
# warnings.filterwarnings("ignore", category=urllib3.InsecureRequestWarning)
urllib3.disable_warnings()

from fusemine import data, converting, representation, linking
# from fusemine import training

from fusemine._utils import (
    DefaultLogger,
    compile_entities,
)

logger = DefaultLogger()
logger.configure("INFO")

class FuseMine:
    def __init__(self, 
                 commodity: str='all',
                 country: str='all', state: str='all',

                 file_input:str=None,
                 dir_output:str=None,
                 dir_entities:str='./fusemine/_entities',

                 location_method:str='distance',
                 text_method:str='classify',
                 start_fresh:bool=False,

                 verbose: bool=False,) -> None:

        # Set default information
        self.commodity = commodity
        self.country = country
        self.state = state

        # End program if information is not correctly added in
        if (self.commodity == 'all') and (self.country == 'all') and (self.state == 'all'):
            raise ValueError("Provide either the focus commodity or textual location to use FuseMine")
        
        if (self.state != 'all') and (self.country == 'all'):
            raise ValueError("Provide both the country name and state name of the desire region to use FuseMine")
        
        self.file_input = file_input
        if not dir_output:
            dir_output = './output'
        self.dir_output = dir_output
        self.dir_entities = dir_entities

        # Convert string input in QNode
        self.set_variables()

        self.start_fresh = start_fresh
        self.location_method = location_method
        self.text_method = text_method

        self.pl_linked_data = {}

        if verbose:
            logger.set_level('DEBUG')
        else:
            logger.set_level('WARNING')

    def set_variables(self) -> None:
        # Load_entity as dataframe
        path_entities_mapfile = os.path.join(self.dir_entities, '_selected_cols.pkl')
        if data.check_exist(path_entities_mapfile) != -1:
            self.dict_entity_cols = data.load_data(path_entities_mapfile, '.pkl')

        self.entities = compile_entities(self.dir_entities, self.dict_entity_cols)
        logger.info(f"Entities dictionary has been created based on those available as CSV in {self.dir_entities}")

        # Convert commodity to commodity QNodes + Sanity Check
        pl_entities_commodity = self.entities['commodity']
        if self.commodity.lower() == 'all':
            self.focus_commodity_id = pl_entities_commodity['minmod_id'].to_list()
            self.focus_commodity = pl_entities_commodity['name'].to_list()

        elif self.commodity.lower() == 'critical':
            tmp_crit_entities = pl_entities_commodity.filter(pl.col('is_critical_commodity') == '1')  # Filter those with is critical commodity == 1
            self.focus_commodity_id = tmp_crit_entities['minmod_id'].to_list()
            self.focus_commodity = tmp_crit_entities['name'].to_list()
            del tmp_crit_entities

        else:
            tmp_crit_entities = pl_entities_commodity.filter(pl.col('name') == self.commodity)

            if not tmp_crit_entities.is_empty():
                self.focus_commodity_id = [tmp_crit_entities.item(0, 'minmod_id')]
                self.focus_commodity = [self.commodity]
            else:
                raise ValueError(f"{self.commodity} is not an acceptable commodity."
                                 f"Refer to 'name' column in {self.dir_entities}/commodity.csv for an appropriate commodity name")

        # Convert state to state QNode
        if self.state != 'all':
            self.state_id = self.entities['state_or_province'].filter(pl.col('country_name') == self.country, pl.col('name') == self.state).item(0, 'minmod_id')
        else:
            self.state_id = None

        # Convert state to state QNode
        if self.country != 'all':
            self.country_id = self.entities['country'].filter(pl.col('name') == self.country).item(0, 'minmod_id')
        else:
            self.country_id = None

    def load_data(self,
                  input_data: str=None,
                  method: str='kg',) -> None:
        
        self.method = method

        if method == 'data':
            self.data = data.get_filedata(input_data=input_data)

        # if method == 'kg':
        Queryer = data.QueryKG()
        self.data = Queryer.get_data(list_commodity_code = self.focus_commodity_id,
                                     country_code = self.country_id,
                                     state_code = self.state_id)        # data: Dict[str, pl.DataFrame]

        logger.info(f"Loaded all data for commodity {self.focus_commodity} located in state {self.state} country {self.country}")

        if not self.start_fresh:
            # TODO: retrieve also sameas links
            self.same_as = None
            logger.info("Sameas links available on minmod has been loaded.")
            logger.info("FuseMine will omit those with sameas links.")

    def prepare_data(self,) -> None:
        """
        TODO: fill in information

        """
        self.gpd_data = {}
        self.pl_data_nl = {}
        for code, pl_data in self.data.items():
            list_not_touched_cols = list({'ms_uri', 'source_id', 'country', 'state_or_province', 'site_name', 'location', 'crs'} & set(list(pl_data.columns)))
            if self.text_method == 'classify':
                # Run text serialization
                # TODO: Check order of text
                list_remaining_cols = list({'country', 'state_or_province', 'location', 'site_name', 'other_names', 'commodity', 'deposit_type'} & set(list(pl_data.columns)))

                pl_data = pl_data.select(
                    pl.col(list_not_touched_cols),
                    text = pl.struct(pl.col(list_remaining_cols).str.to_lowercase()).map_elements(lambda x: converting.text_serialization(x, method='attribute_value_pairs'))
                )

            elif self.text_method == 'cosine':
                # V1 relies only on mineral site name
                suffix_regex = data.load_data(os.path.join(self.dir_entities, '_site_suffix'), '.pkl')
                commodity_regex = data.load_data(os.path.join(self.dir_entities, '_commodities_prefix'), '.pkl')
                self.data[code] = pl_data.select(
                    pl.col(list_not_touched_cols),
                    pl.col(['site_name', 'other_names']).str.replace_all(rf"{suffix_regex}", '').str.replace_all(rf"{commodity_regex}", '').str.replace_all(r"\s+", ' ')
                )

            else:
                # FuseMine V3 combine classification with cosine similarity
                # Aim to mitigate issue with classification based not performing well on foreign named mines
                list_site_names = {'site_name', 'other_names'} & set(list(pl_data.columns))
                pl_data = pl_data.with_columns(
                    pl.col('site_name').alias('org_site_name'),
                    pl.col(list_site_names).str.replace_all(rf"{suffix_regex}", '').str.replace_all(rf"{commodity_regex}", '').str.replace_all(r"\s+", ' '),
                    text = pl.struct(pl.col(list_remaining_cols).str.to_lowercase()).map_elements(lambda x: converting.text_serialization(x, method='attribute_value_pairs')),
                )
            
            # Prepare location attribute
            self.gpd_data[code], self.pl_data_nl[code] = converting.non2geo(pl_data, str_geo_col='location')
    
    def link(self,) -> None:    
        """
        TODO: Fill in information
        """    
        for code, gpd_data_i in self.gpd_data.items():
            # Location_based linking
            if self.location_method == 'distance':
                gpd_data_i = representation.point_rep(gpd_data_i)
                gpd_data_i = linking.group_proximity(gpd_data_i)

            # Convert location grouped to polars df
            pl_data_i = converting.geo2non(gpd_data_i)

            # Merge converted with no location
            pl_data_c = pl.concat(
                [pl_data_i, self.pl_data_nl[code]],
                how='diagonal'
            )

            # Create text dict (used for classification)
            dict_uri_text = converting.non2dict(pl_data=pl_data_c,
                                                key_col='ms_uri', val_col='text')
            # Create name dict (use for guarantee & unnamed/unidentified filtering)
            dict_uri_name = converting.non2dict(pl_data=pl_data_c,
                                                key_col='ms_uri', val_col='org_site_name')

            # Group by state & country
            pl_data_c = linking.group_txt_loc(pl_data_c, link_lvl='state')
            # Group only by country
            pl_data_c = linking.group_txt_loc(pl_data_c, link_lvl='country')

            # Create text compare pairs
            # Priority: grp_loc -> grp_state -> grp_country
            list_loc_based = pl_data_c.filter(
                pl.col('grp_loc') != -1,
                ~pl.col('grp_loc').is_null()).group_by('grp_loc').agg([pl.all()])['ms_uri'].to_list()
            
            list_pl_state = pl_data_c.filter(
                pl.col('grp_state') != -1,
            ).partition_by('grp_state')
            list_pl_state = [(i if i.filter(pl.col('grp_loc').is_null()).shape[0] > 0 else pl.DataFrame()) for i in list_pl_state]

            list_pl_country = pl_data_c.filter(
                pl.col('grp_country') != -1,
                ((~pl.col('grp_loc').is_null()) | (pl.col('grp_state') == -1))
            ).partition_by('grp_country')
            list_pl_country = [(i if i.filter(pl.col('grp_state') == -1).shape[0] > 0 else pl.DataFrame()) for i in list_pl_country]

            for pl_state in list_pl_state:
                if pl_state.is_empty():
                    continue

                tmp_loc = pl_state.filter(
                    ~(pl.col('grp_loc').is_null())
                )['ms_uri'].to_list()
                tmp_non_loc = pl_state.filter(
                    pl.col('grp_loc').is_null()
                )['ms_uri'].to_list()

                if (len(tmp_loc) > 0) and (len(tmp_non_loc) > 0):
                    list_loc_based.append([tmp_loc, tmp_non_loc])
                elif (len(tmp_loc) == 0 ) and (len(tmp_non_loc) > 0):
                    list_loc_based.append(tmp_non_loc)
                        
            pairs = converting.listlist2pairs(list_loc_based)

            pd_loc_based = pd.DataFrame(pairs, columns=['ms_uri_1', 'ms_uri_2'])

            pl_loc_based = pl.from_pandas(pd_loc_based).with_columns(
                ms_uri1_text = pl.col('ms_uri_1').replace(dict_uri_text),
                ms_uri2_text = pl.col('ms_uri_2').replace(dict_uri_text),
                ms_uri1_name = pl.col('ms_uri_1').replace(dict_uri_name),
                ms_uri2_name = pl.col('ms_uri_2').replace(dict_uri_name),
            ).with_columns(
                text = pl.lit('[CLS]') + pl.col('ms_uri1_text') + pl.lit('[SEP]') + pl.col('ms_uri2_text') + pl.lit('[SEP]'),
                bool_name_match = (pl.col('ms_uri1_name') == pl.col('ms_uri2_name')),
            )

            # Filter those with 'Unnamed' and 'Unidenfied' and auto-the result to non-match
            pl_loc_based = pl_loc_based.filter(
                ~((pl.col('ms_uri1_name').str.contains('(?i)unnamed|unidentified')) | (pl.col('ms_uri2_name').str.contains('(?i)unnamed|unidentified')))
            ).drop(['ms_uri1_name', 'ms_uri2_name'])

            # Separate those that have EXACT match on name
            # Very highly likely they are linked
            pl_guaranteed = pl_loc_based.filter(
                pl.col('bool_name_match') == True
            ).with_columns(link_text_result = pl.lit(0))

            pl_loc_based = pl_loc_based.filter(
                pl.col('bool_name_match') == False
            )

            self.model_dir = './fusemine/_model/trained_model/'
            self.id2label = data.load_data('./fusemine/_model/id2label.pkl', '.pkl')

            pl_text_linked = linking.text_pair_classification(
                pl_data = pl_loc_based,
                dir_model=self.model_dir,
                id2label=self.id2label,
            )
            list_cosine_score = linking.text_embedding_cosine(pl_text_linked['combined_names'].to_list())

            pl_text_linked = pl_text_linked.with_columns(
                cosine_score = pl.Series(list_cosine_score)
            )
            # TODO: add the classification score with cosine score
            pl_text_linked = pl_text_linked.group_by(['ms_uri_1', 'ms_uri_2']).agg([pl.all()]).map_elements(lambda x: mean(x))
            # TODO: Do statistical mean on similarity
            # TODO: Add functionality with combining confidence of cosine similarity and classification confidence

            self.pl_linked_data[code] = pl.concat([pl_text_linked, pl_guaranteed], how='diagonal_relaxed')

    def identify_links(self) -> None:
        for code, pl_linked_data in self.pl_linked_data.items():
            pl_linked_data = self.pl_linked_data[code].filter(
                pl.col('link_text_result') == 0
            ).select(
                pl.col(['ms_uri_1', 'ms_uri_2']).str.replace('https://minmod.isi.edu/resource/', ''),
                modified_at = pl.lit(datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'))
            )

            self.data[code] = pl_linked_data

    def save_output(self,
                    save_format: str='CSV') -> None:
        """
        TODO: Fill in information
        """
        list_commodities_code = self.data.keys()

        data.check_directory_path(path_directory=self.dir_output, bool_create=True)

        for idx, c_code in enumerate(list_commodities_code):
            path_output = os.path.join(self.dir_output, f"{self.focus_commodity[idx]}_sameas_{datetime.now(timezone.utc).strftime('%m%d')}.large")

            try:
                data.save_data(self.data[c_code], path_save=path_output, save_format=save_format)
            except:
                logger.warning("Unacceptable save format. Defaulting to pickle.")
                data.save_data(self.data[c_code], path_save=path_output, save_format='pickle')