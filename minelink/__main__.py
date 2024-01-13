from argparse import ArgumentParser
# from torch.cuda import device_count

from minelink.m0_load_input.load_data import load_dir
from minelink.m1_input_preprocessing.preprocess_input import preprocessing

from minelink.m2_intralinking.intralink import intralink
from minelink.m2_intralinking.create_intra_representation import create_intra_rep
from minelink.m3_interlinking.interlink import interlink

from minelink.m4_postprocessing.postprocess_dataframe import postprocessing
from minelink.m5_save_output.save_output import *

def main(args):
    data_zinc = 0
    # list_code = load_dir(path_dir=args.data_dir, bool_dict=True)
    # print(list_code)
	# list_code = ['aa', 'ab']
	# list_code = ['aa', 'ab', 'ad']
	# intralink(list_code, args.use_location_base)
    # preprocessing(list_code)
    # intralink(list_code, args.use_location_base)
    # create_intra_rep(list_code, args.use_location_base)
    # interlink(list_code, args.use_location_base)

    # list_code = ['aa', 'ab']

    # if len(list_code) > 1:
    #     create_intra_rep(list_code, args.use_location_base)
    #     # interlink(list_code, args.use_location_base)
    #     interlink(list_code, True)    # Later change it with text allowed
    #     df_linked = postprocessing(bool_interlink=True)
    # # elif len(list_code) == 1:
    # #     df_linked = postprocessing(bool_interlink=False)

    # save_output_json(df_linked, 'MVT_Zinc_ver1')

if __name__ == '__main__':
    parser = ArgumentParser(description='Linking mineral site')
    parser.add_argument('--data_dir', '-d',
                        help='directory in which the data files(.gdb, .csv, .geojson, .pkl, .json) and data dictionaries are saved')
    parser.add_argument('--use_location_base', '-l',
                        help='use location based method to link data', action='store_true')
    
    main(parser.parse_args())
