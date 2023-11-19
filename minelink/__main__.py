from argparse import ArgumentParser
from torch.cuda import device_count

from minelink.load_save import *
from minelink.linking_modules.linking_initiation import site_linking

def main(args):
    # load_dir_items(args.data_dir)
    # with open('/home/yaoyi/pyo00005/CriticalMAAS/src/data/pkl/testing/MRDS_GBW.pkl', 'rb') as handle:
    #     dataframe = pickle.load(handle)
    # intra_linking(dataframe)
    # close_dir('./temporary')

    count = load_dir_items(args.data_dir)

    err = site_linking(count, args.use_location_base, args.use_full)

    close_dir('./temporary')

if __name__ == '__main__':
    parser = ArgumentParser(description='Linking mineral site')
    parser.add_argument('--data_dir', '-d',
                        help='directory in which the data files(.gdb, .csv, .geojson, .pkl) and data dictionaries are saved')
    parser.add_argument('--use_location_base', '-l',
                        help='use location based method to link data', action='store_true')
    parser.add_argument('--use_full', '-f',
                        help='use full pipeline (location and text based) to link data', action='store_true')
    
    main(parser.parse_args())