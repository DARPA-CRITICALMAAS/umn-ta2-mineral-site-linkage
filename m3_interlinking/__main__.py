import os
import time
import logging
import argparse

logging.basicConfig(filename='fusemine.log', format='%(levelname)s:%(message)s', level=logging.INFO)

def main(args):
    print(args)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Linking mineral site within database and across databases')
    parser.add_argument('--data_dir', '-d',
                        help='directory in which the data files(.gdb, .csv, .geojson, .pkl, .json) and data dictionaries are saved')
    
    main(parser.parse_args())