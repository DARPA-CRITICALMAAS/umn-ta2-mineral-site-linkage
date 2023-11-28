from minelink.m1_PreProcessing.dataframe_preprocessing import preprocessing
# from minelink.m1_PreProcessing.dataframe_postprocessing import postprocessing

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