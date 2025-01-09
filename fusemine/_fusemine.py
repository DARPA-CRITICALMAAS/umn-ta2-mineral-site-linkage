import os
import warnings
import urllib3
import polars as pl
from datetime import datetime, timezone

import time     # TODO: remove later

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=pl.MapWithoutReturnDtypeWarning)
# warnings.filterwarnings("ignore", category=urllib3.InsecureRequestWarning)
urllib3.disable_warnings()

from fusemine import data, converting, representation, linking
from fusemine import training

from fusemine._utils import (
    DefaultLogger,
    compile_entities,
)
import fusemine._save_utils as save_utils

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
                 text_method:str='cosine',
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
        self.dir_ouput = dir_output
        self.dir_entities = dir_entities

        # Convert string input in QNode
        self.set_variables()

        self.start_fresh = start_fresh
        self.location_method = location_method
        self.text_method = text_method

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
                  method: str=None,) -> None:
        
        self.method=method

        if method == 'data':
            self.data = data.get_filedata(input_data=input_data)

        # if method == 'kg':
        Queryer = data.QueryKG()
        start_time = time.time()
        self.data = Queryer.get_data(list_commodity_code = self.focus_commodity_id,
                                     country_code = self.country_id,
                                     state_code = self.state_id)
        print(time.time() - start_time)

        logger.info(f"Loaded all data for commodity {self.focus_commodity} located in state {self.state} country {self.country}")

        if not self.start_fresh:
            # TODO: retrieve also sameas links
            self.same_as = None
            logger.info("Sameas links available on minmod has been loaded.")
            logger.info("FuseMine will omit those with sameas links.")
        
    def prepare_data(self,) -> None:
        """
        
        """
        if self.text_method == 'classify':
            # Run text serialization
            list_not_touched_cols = ['ms_uri', 'source_id', 'location', 'crs']
            list_remaining_cols = list(set(list(self.data.columns)) - set(list_not_touched_cols))
            self.data = self.data.select(
                pl.col(list_not_touched_cols),
                text = pl.struct(pl.col(list_remaining_cols)).map_elements(lambda x: converting.text_serialization(x, method=self.method))
            )

        # TODO: add location preparation
        # Unify location crs system
        # list_data_by_crs = self.data.partition_by('crs')
        # list_converted_data = []

        # for pl_data in list_data_by_crs:
        #     # Convert to geopandas with uniform crs
        #     c = pl_data.item(0, 'crs')

        #     if c:
        #         gpd_data = converting.non2geo(pl_data,
        #                                       str_geo_col='location',
        #                                       crs_val = c)
        #         converting.crs2crs(gpd_data, crs_val = )
    
    def link(self,) -> None:    
        """
        TODO: Fill in information
        """    
        pl_loc = self.data.filter(
            pl.col('location').is_not_null()
        )
        pl_none = self.data.filter(
            pl.col('location').is_null()
        )

        # TODO: add location based linking (geolocation)

        # TODO: convert to pairs

        # TODO: add text location based linking (if country equals, if state_or_province also equals)

        self.data = self.data.with_columns(
            text = pl.lit('[CLS]') + pl.col('ms_uri1_text') + pl.lit('[SEP]') + pl.col('ms_uri2_text') + pl.lit('[SEP]')
        )

        # TODO: check text_based linking
        if self.text_method == 'classify':
            self.model_dir = './fusemine/_model/trained_model/'
            self.id2label = data.load_data('./fusemine/_model/id2label.pkl')

            self.data = linking.text_pair_classification(pl_data=self.data,
                                                         dir_model=self.model_dir,
                                                         id2label=self.id2label)
        else:
            # TODO: add Fuseminev1
            pass

        logger.info(f"Linking completed for commodity {c}")

    def identify_links(self) -> None:
        self.data = self.data.filter(
            pl.col('linked') == 1 
        ).select(
            pl.col(['ms_uri_1', 'ms_uri_2']),
            modified_at = pl.lit(datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'))
        )

    def save_output(self,
                    save_format: str='csv') -> None:
        """
        TODO: Fill in information
        """
        list_commodities = self.data.keys()

        for c in list_commodities:
            path_output = os.path.join(self.dir_output, f"{c}_sameas_{datetime.now(timezone.utc).strftime('%m%d')}.large")

            try:
                data.save_data(self.data, path_save=path_output, save_format=save_format)
            except:
                logger.warning("Unacceptable save format. Defaulting to pickle.")
                data.save_data(self.data, path_save=self.path_output, save_format='pickle')