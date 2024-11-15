import warnings

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

from fusemine._utils import (
    DefaultLogger
)

logger = DefaultLogger()
logger.configure("INFO")

class FuseMine:
    def __init__(self, 
                 commodity: str='Tungsten',
                 verbose: bool=False,
                 ):
        
        # Commodity to link
        self.commodity = commodity

        if verbose:
            logger.set_level
        return 0