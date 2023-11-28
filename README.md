# UMN TA2 Mineral Site Linkage
This repository is for the reconciliation of geospatial datasets (e.g., mineral site databases - MRDS, USMIN)

## Requirements
Install the required package libraries by running the following code in the command line:
```
pip install -r requirements.txt
```

## How to run
Before you run the code, you will need the datasets to be placed under `./data` 
In the [main.py](https://github.com/DARPA-CRITICALMAAS/umn-ta2-mineral-site-linkage/blob/main/main.py) modify the following portion:
```
# Will combine mineral sites available in 'MRDS_Zinc' and 'Taylor' and save it to 'Zinc.json'
df_site, dict_loc, dict_sameas, dict_geo = intra_group('MRDS_Zinc', 'Taylor', 'Zinc')
```
Run the python file by entering the following code in the command line:
```
python3 main.py
```
The linked JSON output would be located under `./outputs` with the name defined in the main file.

```
python -m minelink -d path/to/data/directory [-f or -l]
```

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
|-- m0_SaveAndLoad
|   |-- __init__.py
|   |-- directory_related.py
|   |-- file_related.py
|
|-- m1_PreProcessing
|   |-- __init__.py
|   |-- column_mapping.py                   # Reads the data dictionary and find columns relevant to lat/long/crs
|   |-- converting_to_geodataframe.py
|   |-- dataframe_preprocessing.py
|
|-- m2_LocationBasedLinking
|   |-- __init__.py
|   |-- link_with_loc.py
|
|-- m3_TextBasedLinking
|   |-- __init__.py
|   |-- link_with_all.py
|
|-- m4_InitiateLinking
|   |-- __init__.py
|   |-- determine_link_mode.py
|   |-- inter_linking.py
|   |-- intra_linking.py
|
|-- m5_PostProcessing
|   |-- __init__.py
|   |-- dataframe_postprocessing.py
|   |-- determine_location.py
|
|-- m6_Testing
|   |-- __init__.py
|   |-- evaluation_metrics.py
|
```

## TODO
- [] 