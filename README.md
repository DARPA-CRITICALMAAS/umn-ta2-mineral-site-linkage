# TA2 UMN - FuseMine 
FuseMine is the pipeline for data reconciliation from multiple mineral site databases (e.g., USMIN and MRDS) and mineral sites extracted from the reports (i.e., output from TA2 InferLink/USC).

## Requirements
### Mineral Site Databases & Data Dictionaries
FuseMine requires a data directory that consists of one or more mineral site database and their corresponding data dictionary (i.e., attribute name and description). 

The following is an example of data directory layout:
```
./fusemine/data
|-- MRDS.csv
|-- dict_MRDS.csv
|-- USMIN.csv
|-- dict_USMIN.csv
```

* The file name of the mineral site databases must be the source name (e.g., MRDS, USMIN, {DOI}), and the corresponding data dictionary file must have a file name of dict_{source name}. 
* The data dictionary must consist of pairs of descriptions and attribute names in the database.
  
    | attribute_label | short_description | attribute_definition |
    | --- | --- | --- |
    | country | Country name | Name of the country in which the site is located. Textual values of no more than 20 characters. |
    | latitude | Latitude | Geographic latitude of the site, WGS84 if needed. Real numbers stored in double precision. |
    * Data dictionaries are usually available inside the compressed zip data file (e.g., [MRDS](https://mrdata.usgs.gov/mrds/mrds-csv.zip)).

### Libraries
The code requires `python >= 3.8` and `pytorch >= 2.0`. `CUDA >= 11.7` is recommended when running FuseMine.
Install the required package libraries by running the following code in the command line:
```
pip install -r requirements.txt
```

## How to run
### Run FuseMine End-to-End
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
| `INTRALINK_BOUNDARY` | Intralink Boundary | 500 (meters) |
| `INTERLINK_BUFFER` | Interlink Buffer | 5000 (meters) |
| `INTERLINK_OVERLAP` | Interlink Overlap Area | 1 (meters squared) |
| `THRESHOLD_SIMILARITY` | Textual Similarity Threshold | 0.74 |
<!-- | `EMBEDDING_RATIO1` | Ratio of Name Embedding | 0.71 |
| `EMBEDDING_RATIO2` | Ratio of Commodity Embedding | 0.29 | -->
