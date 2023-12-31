from argparse import ArgumentParser

from minelink.m0_save_and_load.save_load_directory import remove_dir
from minelink.m6_determine_linking_method.determine_link_mode import site_linking
from minelink.m7_postprocessing.dataframe_postprocessing import postprocessing

def main(args):
    print("here")

if __name__ == '__main__':
    parser = ArgumentParser(description='Module accuracy/time check')
    parser.add_argument('--module', '-,m',
                        help='module number that will be tested')
    parser.add_argument('--data_dir', '-d',
                        help='directory in which the data files(.gdb, .csv, .geojson, .pkl) and data dictionaries are saved')
    parser.add_argument('--use_location_base', '-l',
                        help='use location based method to link data', action='store_true')
    
    main(parser.parse_args())