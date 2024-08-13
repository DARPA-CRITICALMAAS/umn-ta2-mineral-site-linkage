# FuseMine
## Overview
<!-- FuseMine is a pipeline designed for data reconciliation across multiple mineral site databases (e.g., USMIN and MRDS) and mineral sites extracted from reports (e.g., output from TA2 InferLink/USC). FuseMine facilitates the reconciliation of mineral sites by leveraging both location-based data and semantic similarities in textual attributes such as site name and commodity. -->

FuseMine is a comprehensive pipeline designed to link data across multiple mineral site databases, such as USMIN and MRDS, and mineral site records extracted from reports and tables through tools created by TA2 Inferlink and USC. It facilitates the reconciliation process by utilizing both geospatial data and semantic similarities in textual attributes, such as site name and commodity. 

<!-- FuseMine offers two primary functions: data processing and data reconciliation. In the data processing phase, raw structured data is converted into a [standardized schema](https://github.com/DARPA-CRITICALMAAS/schemas/tree/main/ta2) compatible with the Knowledge Graph (KG), where TA2 stores all processed data. During the data reconciliation phase, FuseMine queries the KG based on the commodity and links records representing the same mineral site.  -->

## Installation
1. Clone this repository
```
git clone https://github.com/DARPA-CRITICALMAAS/umn-ta2-mineral-site-linkage.git
cd umn-ta2-mineral-site-linkage
```

2. FuseMine recommends allocating more than 10GB of memory. The code requires `python >= 3.10` and `pytorch >= 2.0`. The following is the example command line for creating a conda environment with `fusemine` name and `python=3.10`

```
conda create -n fusemine python=3.10
conda activate fusemine
```

3. Installation instructions for pytorch can be found [here](https://pytorch.org/get-started/locally/). Installing with CUDA support is highly recommended but not required for running FuseMine.


4. Install the required package libraries by running the following command line:
```
pip install -r requirements.txt
```

## Usage
### FuseMine Arguments
The following command-line arguments are available to customize FuseMine:
- `--commodity`: Specific commodity to focus on.
- `--single_stage`: Method for location-based single-stage linking.
- `--intralink`: Method for location-based intralinking.
- `--interlink`: Method for location-based interlinking.
- `--same_as_directory`: Directory to store the same as CSV files (default: `./output`).
- `--same_as_filename`: Filename of the same as CSV file (default: `./<commodity>_sameas.csv`).
- `--tungsten`: Run evaluation with tungsten.

### Example FuseMine Commands
Before running FuseMine, ensure that all data is available on the [MinMod Knowledge Graph](https://minmod.isi.edu/) (MinMod KG). 

To run FuseMine by using distance-based inra-linking and area-based inter-linking methoeds, use the following command: 
```
python3 fusemine.py --commodity <commodity_name> --intralink distance --interlink area
```
The output file will be stored in the default location (`./outputs/`). 

To evaluate the performance of FuseMine on Idaho/Montana region Tungsten assessment data [(Goldman et al., 2020)](https://www.sciencebase.gov/catalog/item/5f1f058682cef313ed8e9e91), use the following:
```
python3 fusemine.py --tungsten
```

<!-- and Great Basic Area Tungsten assessment data [(et al., year)](link) -->

#### FuseMine Parameters
The following table lists all the parameters used in the pipeline. These values can be modified in the [`params.ini`](https://github.com/DARPA-CRITICALMAAS/umn-ta2-mineral-site-linkage/blob/main/params.ini).

| Name | Description | Value |
| --- | --- | --- |
| `POINT_BUFFER_UNIT_METER` | Maximum distance between two geocoordinate points to be considered as a same site | 2500 (meters) |
| `POLYGON_BUFFER_UNIT_METER` | Size of buffer to be added around a geometry | 5000 (meters) |
| `POLYGON_AREA_OVERLAP_UNIT_SQMETER` | Minimum intersection-over-union (IOU) two geometries must have to be considered as a same site | 0.5 |
| `ATTRIBUTE_VALUE_THRESHOLD` | Minimum cosine similarity distance two text embeddings must have to be considered as a similar text | 0.65 |

### Raw Data Processing
To populate additional raw structured data to MinMod KG, create the attribute map file (.CSV), which maps headers of raw structured data to [MinMod KG schema](https://github.com/DARPA-CRITICALMAAS/schemas/tree/main/ta2). Here is an example of attribute map CSV: [`sample_mapfile.csv`](https://github.com/DARPA-CRITICALMAAS/umn-ta2-mineral-site-linkage/blob/main/sample_mapfile.csv).

After creating an attribute map CSV, run the following command line:
```
python3 process_data_to_schema.py --raw_data <path_to_raw_CSV> --attribute_map <path_to_attribute_map> --schema_output_directory <path_to_output_directory> --schema_output_filename <output_file_name>
```
The following command-line arguments are required for data preprocessing:
- `--raw_data`: Directory or file where the raw mineral site databases are located.
- `--attribute_map`: CSV file with label mapping information (see sample_mapfile.csv for reference).
- `--schema_output_directory`: Directory where the processed raw mineral site database will be stored.
- `--schema_output_filename`: Filename for the processed raw mineral site database.

Upload the processed JSON file to the [MinMod Data Repository](https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data/tree/main/data). Once the data is merged to main, it takes roughly 2 hours for the update to be reflected on the KG.