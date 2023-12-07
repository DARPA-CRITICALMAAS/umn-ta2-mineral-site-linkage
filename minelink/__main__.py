from argparse import ArgumentParser
from torch.cuda import device_count

from minelink.m8_save_output.remove_tmp_dir import remove_dir
from minelink.m6_determine_linking_method.determine_link_mode import site_linking
from minelink.m7_postprocessing.dataframe_postprocessing import postprocessing

def main(args):
    site_linking(args.data_dir, bool_location=args.use_location_base)
    postprocessing()
    remove_dir()

if __name__ == '__main__':
    parser = ArgumentParser(description='Linking mineral site')
    parser.add_argument('--data_dir', '-d',
                        help='directory in which the data files(.gdb, .csv, .geojson, .pkl) and data dictionaries are saved')
    parser.add_argument('--use_location_base', '-l',
                        help='use location based method to link data', action='store_true')
    
    main(parser.parse_args())