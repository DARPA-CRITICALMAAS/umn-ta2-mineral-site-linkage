from minelink.linking_modules.dataframe_formatting import *

def site_linking(count, bool_location, bool_full):
    if count <= 0:
        print("ENDING PROGRAM: No files to process")
        return -1
    
    print("Processing", count, "files")
    print(bool_location, bool_full)
    if bool_full:
        print("Full based")
    elif bool_location:
        print("Location based")
    
    print("ENDING PROGRAM: No linking mode selected")
    return -1