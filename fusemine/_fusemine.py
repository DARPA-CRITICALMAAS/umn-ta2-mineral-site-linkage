import os
import warnings
import polars as pl
from datetime import datetime, timezone

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

from fusemine import data, converting, linking

from fusemine._utils import (
    DefaultLogger
)
import fusemine._save_utils as save_utils

logger = DefaultLogger()
logger.configure("INFO")

class FuseMine:
    def __init__(self, 
                 commodity: str='all',
                 country: str='all', state: str='all',

                 dir_output:str=None,
                 dir_entities:str='./fusemine/_entities',

                 location_method:str='distance',
                 text_method:str='cosine',
                 start_fresh:bool=False,

                 verbose: bool=False,) -> None:
        
        if (commodity == 'all') and (country == 'all') and (state == 'all'):
            raise ValueError("Provide either the focus commodity or textual location to use FuseMine")
        
        if (state != 'all') and (country == 'all'):
            raise ValueError("Provide both the country name and state name of the desire region to use FuseMine")

        # TODO: Load entities
        self.entities = None

        # TODO: Check with item names being title case

        # Convert commodity to commodity QNodes + Sanity Check
        pl_entities_commodity = self.entities['commodity']
        if self.commodity.lower() == 'all':
            self.focus_commodity_id = pl_entities_commodity['minmod_id'].to_list()
            self.focus_commodity = pl_entities_commodity['name'].to_list()

        elif self.commodity.lower() == 'critical':
            tmp_crit_entities = pl_entities_commodity.filter(pl.col('is_critical_commodity') == 1)  # Filter those with is critical commodity == 1
            self.focus_commodity_id = tmp_crit_entities['minmod_id'].to_list()
            self.focus_commodity = tmp_crit_entities['name'].to_list()
            del tmp_crit_entities

        else:
            tmp_crit_entities = pl_entities_commodity.filter(pl.col('name') == commodity)

            if tmp_crit_entities:
                self.focus_commodity_id = [tmp_crit_entities.item(0, 'minmod_id')]
                self.focus_commodity = [commodity]
            else:
                raise ValueError(f"{self.commodity} is not an acceptable commodity."
                                 f"Refer to 'name' column in {dir_entities}/commodity.csv for an appropriate commodity name")

        # Load information about textual location (country)
        if self.country != 'all':
            self.country_id = self.entities['country'].filter(pl.col('name') == country).item(0, 'minmod_id')
        else:
            self.country_id = None
        self.country = country

        # Load informationa about textual location (state)
        if self.state != 'all':
            self.state_id = self.entities['state_or_province'].filter(pl.col('country_name') == country, pl.col('name') == state).item(0, 'minmod_id')
        else:
            self.state_id = None
        self.state = state

        self.start_fresh = start_fresh
        self.location_method = location_method
        self.text_method = text_method

        if verbose:
            logger.set_level('DEBUG')
        else:
            logger.set_level('WARNING')

    def load_data(self,
                  method: str='kg',
                  input_data: str=None) -> None:
        
        self.method=method

        if method == 'kg':
            Queryer = data.QueryKG()
            self.data = Queryer.get_data(self.commodity_code)
        
        elif method == 'data':
            self.data = data.get_filedata()

        if not self.start_fresh:
            # TODO: retrieve also sameas links
            self.same_as = None

        logger.info(f"Loaded all data for commodity {self.focus_commodity} located in state {self.state} country {self.country}")
        
    def prepare_data(self,) -> None:
        if self.text_method == 'cosine':
            # Run text serialization
            pass

        list_data_by_crs = self.data.partition_by('crs')
        list_converted_data = []

        for pl_data in list_data_by_crs:
            # Convert to geopandas with uniform crs
            c = pl_data.item(0, 'crs')

            if c:
                gpd_data = converting.non2geo(pl_data,
                                              str_geo_col='location',
                                              crs_val = c)
                converting.crs2crs(gpd_data, crs_val = )
        pass
    
    def link(self,) -> None:        
        # linking.

        pass

    def save_output(self,
                    save_format: str='csv') -> None:
        """
        TODO: Fill in information
        """
        path_output = os.path.join(self.dir_output, self.file_output)

        try:
            data.save_data(self.data, path_save=path_output, save_format=save_format)
        except:
            logger.warning("Unacceptable save format. Defaulting to pickle.")
            data.save_data(self.data, path_save=self.path_output, save_format='pickle')