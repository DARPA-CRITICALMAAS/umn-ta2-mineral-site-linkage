from argparse import ArgumentParser
from torch.cuda import device_count

from minelink.m0_SaveAndLoad.save_load_directory import *
# from minelink.m2_LocationBasedLinking.linking_initiation import site_linking
# from minelink.m4_InitiateLinking.determine_link_mode import *

def main(args):
    file_count = open_directory(args.data_dir)
    print(args.use_location_base, args.use_full)
    # count = load_dir_items(args.data_dir)

    # err = site_linking(count, args.use_location_base, args.use_full)

    # close_dir('./temporary')

if __name__ == '__main__':
    parser = ArgumentParser(description='Linking mineral site')
    parser.add_argument('--data_dir', '-d',
                        help='directory in which the data files(.gdb, .csv, .geojson, .pkl) and data dictionaries are saved')
    parser.add_argument('--use_location_base', '-l',
                        help='use location based method to link data', action='store_true')
    parser.add_argument('--use_full', '-f',
                        help='use full pipeline (location and text based) to link data', action='store_true')
    
    main(parser.parse_args())