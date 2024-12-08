import warnings

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

from fusemine import data
from fusemine.converting import *
from fusemine._utils import (
    DefaultLogger
)
import fusemine._save_utils as save_utils

logger = DefaultLogger()
logger.configure("INFO")

class FuseMine:
    def __init__(self, 
                 commodity: str|int='Tungsten',
                 country: str='ALL',
                 state: str='ALL',
                 verbose: bool=False) -> None:
        
        if not commodity:
            # TODO: get all
            pass
        elif isinstance(commodity, int):
            # TODO: get data for all is critical commodity = commodity
            pass

        self.commodity_code = entity2id(commodity)
        self.country_code = entity2id(country)
        self.state_code = entity2id(state)

        if verbose:
            logger.set_level('DEBUG')
        else:
            logger.set_level('WARNING')

    def link_records():
        pass

    def load_data(self,
                  method: str='kg'):
        

        self.method=method

        if method == 'kg':
            Queryer = data.QueryKG()

            self.data = Queryer.get_data(self.commodity_code)
        
        elif method == 'data':
            self.data = data.get_filedata()
        
    def linking_spatial(self,
                        method: str='distance'):
        return 0
    
    def linking_textual(self,
                        method: str='cosine'):
        
        return 0

    def save_data():
        return 0

    def save_links(self):
        return 0