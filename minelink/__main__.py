from argparse import ArgumentParser
from torch.cuda import device_count

from minelink.load_save import *

def main(args):
    load_dir_items(args.data_dir)

if __name__ == '__main__':
    parser = ArgumentParser(description='Linking mineral site')
    parser.add_argument('--data_dir', '-d',
                        help='directory in which the data files(.gdb, .csv, .geojson, .pkl) are saved')
    
    main(parser.parse_args())