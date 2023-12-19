from argparse import ArgumentParser
from torch.cuda import device_count

from minelink.m0_load_input.load_data import load_dir, load_file
from minelink.m1_input_preprocessing.preprocess_input import preprocessing
from minelink.m2_intralinking.intralink import just_dimension

def main(args):
    # list_code = load_dir(path_dir=args.data_dir, bool_dict=True)
    # preprocessing(list_code)
    just_dimension('aa')
    

if __name__ == '__main__':
    parser = ArgumentParser(description='Linking mineral site')
    parser.add_argument('--data_dir', '-d',
                        help='directory in which the data files(.gdb, .csv, .geojson, .pkl, .json) and data dictionaries are saved')
    parser.add_argument('--use_location_base', '-l',
                        help='use location based method to link data', action='store_true')
    
    main(parser.parse_args())