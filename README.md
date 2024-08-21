# FuseMine
Documentation available in PDF format [here](https://github.com/DARPA-CRITICALMAAS/umn-ta2-mineral-site-linkage/blob/main/FuseMine%20Usage%20Documentation.pdf)

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

2. Memory requirement depends on the size of the data; however, FuseMine recommends allocating at least 10GB of memory for any task. The code requires `3.10 <= python < 3.12` and `pytorch >= 2.0`. The following is the example command line for creating a conda environment with `fusemine` name and `python=3.10`

```
conda create -n fusemine python=3.10
conda activate fusemine
conda install pip
```

3. Installation instructions for PyTorch can be found [here](https://pytorch.org/get-started/locally/). Installing with CUDA support is highly recommended but not required for running FuseMine.


4. Install the necessary package libraries by running the following command line:
```
pip install -r requirements.txt
```

*Note: FuseMine requires an active network connection throughout its execution.*

## Usage
### Data Processing
The data processing step requires a manually curated attribute map file, which maps the headers of the raw structured data to the MinMod KG schema. An example of an attribute map CSV is provided: [sample_mapfile.csv](https://github.com/DARPA-CRITICALMAAS/umn-ta2-mineral-site-linkage/blob/main/sample_mapfile.csv).

<!-- ### Raw Data Processing
To populate additional raw structured data to MinMod KG, create the attribute map file (.CSV), which maps headers of raw structured data to [MinMod KG schema](https://github.com/DARPA-CRITICALMAAS/schemas/tree/main/ta2). Here is an example of attribute map CSV: [`sample_mapfile.csv`](https://github.com/DARPA-CRITICALMAAS/umn-ta2-mineral-site-linkage/blob/main/sample_mapfile.csv). -->

After creating the attribute map CSV, use the following command to process the data:
```
python3 process_data_to_schema.py --raw_data <path_to_raw_CSV> --attribute_map <path_to_attribute_map> --schema_output_directory <path_to_output_directory> --schema_output_filename <output_file_name>
```
#### Description of Arguments
<!-- The following command-line arguments are required for data preprocessing: -->
- `--raw_data`: Path (either file or directory) to the raw mineral site database.
- `--attribute_map`: Path to the CSV file containing label mapping information (refer to [sample_mapfile.csv](https://github.com/DARPA-CRITICALMAAS/umn-ta2-mineral-site-linkage/blob/main/sample_mapfile.csv) for guidance)
- `--schema_output_directory`: Directory where the processed database will be saved.
- `--schema_output_filename`: Filename for the processed database.

<!-- Upload the processed JSON file to the [MinMod Data Repository](https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data/tree/main/data). Once the data is merged to main, it takes roughly 2 hours for the update to be reflected on the KG. -->

### Data Reconciliation
Before running FuseMine, ensure that all data is available on the [MinMod Knowledge Graph](https://minmod.isi.edu/) (MinMod KG). The last update time and version of MinMod KG can be checked [here](https://minmod.isi.edu/resource/kg).

To perform data reconciliation using distance-based intralinking and area-based interlinking methods, execute the following command: 
```
python3 fusemine.py --commodity <commodity_name> --intralink distance --interlink area
```

#### Description of Arguments
<!-- The following command-line arguments are available to customize FuseMine: -->
- `--commodity`: The specific commodity to focus on. The commodity name must match one of the MRDS commodity names listed [here](https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data/blob/main/data/entities/commodity.csv).
- `--single_stage`: Method to use for location-based single-stage linking (options: *distance* / *area*)
- `--intralink`: Method to use for location-based intralinking (options: *distance* / *area*)
- `--interlink`: Method to use for location-based interlinking (options: *distance* / *area*)
- `--same_as_directory`: Directory where the output “same as” CSV files will be stored (default: `./output`).
- `--same_as_filename`: Filename  for the “same as” CSV file (default: `./<commodity>_sameas.csv`).
<!-- - `--tungsten`: Run evaluation with tungsten. -->

FuseMine generates a log file that reports the number of available records on the MinMod KG, the number of identified mineral sites, and the run time. These log files are saved in the `./logs` directory.

<!-- ### Example FuseMine Commands -->
<!-- Before running FuseMine, ensure that all data is available on the [MinMod Knowledge Graph](https://minmod.isi.edu/) (MinMod KG). 

The output file will be stored in the default location (`./outputs/`).  -->

### Evaluation
To evaluate FuseMine’s performance on Tungsten assessment data from the Idaho/Montana region ([Goldman et al., 2020](https://www.sciencebase.gov/catalog/item/5f1f058682cef313ed8e9e91)), run the following command:

```
python3 fusemine.py --tungsten
```
The evaluation provides group-wise accuracy and link-wise F1 scores, compared against a baseline method that uses only location data. 

<!-- #### FuseMine Parameters
The following table lists all the parameters used in the pipeline. These values can be modified in the [`params.ini`](https://github.com/DARPA-CRITICALMAAS/umn-ta2-mineral-site-linkage/blob/main/params.ini).

| Name | Description | Value |
| --- | --- | --- |
| `POINT_BUFFER_UNIT_METER` | Maximum distance between two geocoordinate points to be considered as a same site | 2500 (meters) |
| `POLYGON_BUFFER_UNIT_METER` | Size of buffer to be added around a geometry | 5000 (meters) |
| `POLYGON_AREA_OVERLAP_UNIT_SQMETER` | Minimum intersection-over-union (IOU) two geometries must have to be considered as a same site | 0.5 |
| `ATTRIBUTE_VALUE_THRESHOLD` | Minimum cosine similarity distance two text embeddings must have to be considered as a similar text | 0.65 | -->

## Data Upload
To ensure that the outputs of FuseMine, whether processed raw data or linkage information, are reflected in the KG, they must be uploaded to the TA2 data repository and merged into the main branch.

**File Organization:**
- Place all processed raw data in the [`umn`](https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data/tree/main/data/umn) folder.
- Place all linkage information in the [`sameas` folder](https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data/tree/main/data/umn/sameas) folder.


**Adding Additional Data:**
- To add a new mineral site record or linkage information upload the new file to the appropriate folder in the repository.
- For detailed steps on adding a file to the repository, refer to the [instructions](https://docs.github.com/en/repositories/working-with-files/managing-files/adding-a-file-to-a-repository) on the official GitHub documentation. 


**Updating Existing Data:**
- To update existing data on the KG, delete the previous file from the repository and then upload the updated file.

Once the data is merged into the main branch, it typically takes 5 to 10 minutes for the changes to be reflected on the KG.
