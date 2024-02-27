import os
import time
import logging
import argparse

# Initializing logging file for preprocessing
logging.basicConfig(filename='fusemine_interlinking.log', format='%(levelname)s:%(message)s', level=logging.INFO)

def main(args):
    logging.info(f'Interlinking process started')

    print(args)

    logging.info(f'Interlinking process ended')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Linking mineral site within database and across databases')
    parser.add_argument('--data_dir', '-d',
                        help='directory in which the data files(.gdb, .csv, .geojson, .pkl, .json) and data dictionaries are saved')
    
    main(parser.parse_args())