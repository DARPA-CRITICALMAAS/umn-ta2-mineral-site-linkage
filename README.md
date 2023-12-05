# UMN TA2 Mineral Site Linkage
This repository is for the reconciliation of geospatial datasets (e.g., mineral site databases - MRDS, USMIN)

## Requirements
Install the required package libraries by running the following code in the command line:
```
pip install -r requirements.txt
```

## How to run
Run the python file by entering the following code in the command line:
```
python -m minelink -d path/to/data/directory [-l if only using geolocation for linking]
```
The linked JSON output would be located under `./outputs`.

### Testing
```
python -m minelink.tester -p path/to/ground_truth_file -c column_with_linking
```

## Directory Layout
```
./minelink
|-- __init__.py
|-- __main__.py
|
|-- m0_save_and_load
|   |-- __init__.py
|   |-- directory_related.py            # 
|   |-- file_related.py                 # 
|
|-- m1_preprocessing
|   |-- __init__.py
|   |-- column_mapping.py               # Reads the data dictionary and find columns relevant to lat/long/crs
|   |-- converting_to_geodataframe.py   #
|   |-- dataframe_preprocessing.py      #
|
|-- m2_location_based_linking
|   |-- __init__.py
|   |-- link_with_loc.py                # Completes linking based on geolocation information
|
|-- m3_text_based_linking
|   |-- __init__.py
|   |-- link_with_all.py                # Completes linking with geolocation and textual information
|   |-- string_match.py                 # Using string_match for textual information
|
|-- m4_intralinking
|   |-- __init__.py
|   |-- intra_linking.py                # Completes linking within databases
|
|-- m5_interlinking
|   |-- __init__.py
|   |-- create_representation.py        # Creates a representation of intra-linked clusters
|   |-- inter_linking.py                # Completes linking across databases
|
|-- m6_determine_linking_method
|   |-- __init__.py
|   |-- determine_link_mode.py          # 
|
|-- m7_postprocessing
|   |-- __init__.py
|   |-- dataframe_postprocessing.py     # Formats grouped database into form required for JSON output
|   |-- determine_location.py           # Determines the most confident location
|
|-- m8_testing
|   |-- __init__.py
|   |-- evaluation_metrics.py           # 
|
```

## Updates
### In Progress
- [ ] Develop accuracy testing method for column mapping (column identification)
- [ ] Make logging available for each module
- [ ] Create method to create short description when there is no column representing short description

### Completed (For: December 6th)
- [x] Create data dictionary refering method
- [x] Populate data dictionary processing
- [x] Update same as structure
- [x] Update postprocessing to reflect same as structure
- [x] Create a function that will determine the CRS value from the description of latitude
- [x] Bring back the column identification function
- [x] Finish function for developing information dictionary

### To-Do
- [ ] Populate text based linking method (create python file for each method)
- [ ] Finish m7.determine_location to select most confident location (not just the first)
- [ ] Develop a clustering algorithm that will allow inter-linking
- [ ] Develop method to test accuracy of each module (column_mapping, linking procedure)

### Archive