from minelink.m1_PreProcessing.dataframe_preprocessing import preprocessing
from minelink.m2_LocationBasedLinking.link_with_loc import *
from minelink.m3_TextBasedLinking.link_with_all import *

def site_linking(count, bool_location, bool_full):
    if count <= 0:
        print("ENDING PROGRAM: No files to process")
        return -1
    
    if not bool_full and not bool_location:
        print("ENDING PROGRAM: No linking mode selected")
        return -1
    
    # print("\nLinking", count, "files:")
    preprocessing(bool_full, bool_location)

    return 0