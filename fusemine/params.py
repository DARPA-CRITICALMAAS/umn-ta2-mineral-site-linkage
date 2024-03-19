ACCEPTABLE_INPUT_FILE = [
                            '.csv',
                            '.gdb',
                            '.pkl',
                            '.geojson',
                            '.json',
                            '.xls',
                            '.xlsx',
                        ]

ACCEPTED_GEOMETRY_FORMAT = [
                                'POINT(.*)', 
                                'LINESTRING([.*])', 
                                'POLYGON([.*])', 
                                'MULTIPOINT([(.*)+])',
                            ]

ATTRIBUTE_MINERAL_SITE = [
                            'idx', 
                            'source_id', 
                            'record_id', 
                            'name', 
                            'location', 
                            'crs', 
                            'country', 
                            'state_or_province'
                          ]

PATH_SRC_DIR = './fusemine/src/'
PATH_TMP_DIR = './temporary/'
PATH_OUTPUT_DIR = './outputs/'

INTRALINK_BOUNDARY = 0.05 # Units in kilometers
INTERLINK_BUFFER = 5000 # Units in meters
INTERLINK_OVERLAP = 1 # Units in meters squared

THRESHOLD_SIMILARITY = 0.74
EMBEDDING_RATIO1 = 0.71
EMBEDDING_RATIO2 = 0.29