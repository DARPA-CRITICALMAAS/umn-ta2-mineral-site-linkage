# UMN TA2 Mineral Site Linkage
This repository is for the reconciliation of geospatial datasets (e.g., mineral site databases - MRDS, USMIN)

## Requirements
Install the required package libraries by running the following code in the command line:
```
pip install -r requirements.txt
```

## How to run
### Run End-to-End
Run the python file by entering the following code in the command line:
```
python -m fusemine [-d path/to/data/directory] [-l if only using geolocation for linking]
```

<!-- ### Process Raw Database to Mineral Site Schema Format
```
cd m1_preprocessing
python process_rawdb_to_schema.py [-d path/to/data/directory] [-u if want to use a predefined attribute mapping]
```
To define the corresponding attribute label, modify the `resource/attribute_map.csv`
If using the predefined mapped attribute file, the items located under 'Matching Attributes in Database' must exist in the database that is being processed.

### Run Intralinking Model
Use the following code if you are using mineral site data in local storage:
```
cd m2_intralinking
python intralinking.py [-d path/to/data/directory] [-l if only using geolocation for linking] [-g if want to save file also as a geojson output]
```

### Run Interlinking Model
```
cd m3_interlinking
python interlinking.py [-d path/to/data/directory] [-l if only using geolocation for linking] [-g if want to save file also as a geojson output] [-o to state output file name; default='interlinked']
``` -->

<!-- ## Directory Layout -->
<!-- ```
./
|-- fusemine.py
|-- params.ini
|
|-- m0_loading_and_saving
|   |-- loading_local_data.py                   # Loads the data available on the user local storage
|   |-- loading_kg_data.py                      # Loads the data available on the knowledge graph
|   |-- save_sameas_output.py                   # Saves the reconciliation output as a two-column URI CSV output
|   |-- save_to_geojson_output.py               # Saves the output as a geojson file that can plotted on a GIS software
|   |-- save_to_json_output.py                  # Saves the output as a json file that can be loaded on the knowledge graph
|
|-- m1_preprocessing
|   |-- compare_attribute_def_similarity.py     # 
|   |-- extract_attributes_from_db.py           #
|   |-- process_gpkg_to_json.py
|   |-- process_rawdb_to_schema.py              # Processes the raw database available in the local directory to a local schema format
|
|-- m2_intralinking
|   |-- intralinking.py                         # 
|   |-- location_based_intralinking.py          #
|   |-- text_based_intralinking.py              #
|
|-- m3_interlinking
|   |-- interlinking.py                         #
|   |-- location_based_intralinking.py          #
|
|-- resource
|   |-- attribute_dictionary.pkl                # Previously identified attribute labels
|   |-- crs.pkl                                 # List of coordinate reference systems (CRS)
|
|-- utils
|   |-- loading
|   |-- saving
|   |-- geolocation

``` -->



### Parameters
The following portion lists all the parameters that are used in the pipeline. These values can be modified in the `params.py` file.

| Name | Description | Value |
| --- | --- | --- |
| `INTRALINK_BOUNDARY` | Intralink Boundary | 0.5 (kilometers) |
| `INTERLINK_BUFFER` | Interlink Buffer | 15 (meters) |
| `INTERLINK_OVERLAP` | Interlink Overlap Area | 1 (meters squared) |
| `THRESHOLD_SIMILARITY` | Textual Similarity Threshold | 0.74 |
<!-- | `EMBEDDING_RATIO1` | Ratio of Name Embedding | 0.71 |
| `EMBEDDING_RATIO2` | Ratio of Commodity Embedding | 0.29 | -->