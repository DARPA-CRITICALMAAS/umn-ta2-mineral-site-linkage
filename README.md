# FuseMine
Documentation available in PDF format [here](https://github.com/DARPA-CRITICALMAAS/umn-ta2-mineral-site-linkage/blob/main/FuseMine%20Usage%20Documentation.pdf)

## Overview
FuseMine is a comprehensive pipeline designed to link data across multiple mineral site databases, such as USMIN and MRDS, and mineral site records extracted from reports and tables through tools created by TA2 Inferlink and USC. It facilitates the reconciliation process by utilizing both geospatial data and semantic similarities in textual attributes, such as site name and commodity. 

## Installation
1. Clone this repository
```
git clone https://github.com/DARPA-CRITICALMAAS/umn-ta2-mineral-site-linkage.git
cd umn-ta2-mineral-site-linkage
```

2. Memory requirement depends on the size of the data; however, FuseMine recommends allocating at least 10GB of memory for any task. Install the necessary package libraries by running the following command line:
```
poetry install
```
*Note: FuseMine requires an active network connection throughout its execution.*

## Usage
### Data Processing
The data processing step requires a manually curated attribute map file, which maps the headers of the raw structured data to the MinMod KG schema. An example of an attribute map CSV is provided: [sample_mapfile.csv](https://github.com/DARPA-CRITICALMAAS/umn-ta2-mineral-site-linkage/blob/main/sample_mapfile.csv).

After creating the attribute map CSV, use the following command to process the data:
```
poetry run python3 process_data_to_schema.py --raw_data <path_to_raw_CSV> --attribute_map <path_to_attribute_map> --schema_output_directory <path_to_output_directory> --schema_output_filename <output_file_name>
```
#### Description of Arguments
<!-- The following command-line arguments are required for data preprocessing: -->
- `--raw_data`: Path (either file or directory) to the raw mineral site database.
- `--attribute_map`: Path to the CSV file containing label mapping information (refer to [sample_mapfile.csv](https://github.com/DARPA-CRITICALMAAS/umn-ta2-mineral-site-linkage/blob/main/sample_mapfile.csv) for guidance)
- `--schema_output_directory`: Directory where the processed database will be saved.
- `--schema_output_filename`: Filename for the processed database.

### Data Reconciliation
Before running FuseMine, ensure that all data is available on the [MinMod Knowledge Graph](https://minmod.isi.edu/) (MinMod KG). The last update time and version of MinMod KG can be checked [here](https://minmod.isi.edu/resource/kg).

To perform data reconciliation using distance-based intralinking and area-based interlinking methods, execute the following command: 
```
poetry run python3 fusemine.py --commodity <commodity_name> --intralink distance --interlink area
```

#### Description of Arguments
<!-- The following command-line arguments are available to customize FuseMine: -->
- `--commodity`: The specific commodity to focus on. The commodity name must match one of the MRDS commodity names listed [here](https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data/blob/main/data/entities/commodity.csv).
- `--single_stage`: Method to use for location-based single-stage linking (options: *distance* / *area*)
- `--intralink`: Method to use for location-based intralinking (options: *distance* / *area*)
- `--interlink`: Method to use for location-based interlinking (options: *distance* / *area*)
- `--same_as_directory`: Directory where the output “same as” CSV files will be stored (default: `./output`).
- `--same_as_filename`: Filename  for the “same as” CSV file (default: `./<commodity>_sameas.csv`).

FuseMine generates a log file that reports the number of available records on the MinMod KG, the number of identified mineral sites, and the run time. These log files are saved in the `./logs` directory.

### Evaluation
To evaluate FuseMine’s performance on Tungsten assessment data from the Idaho/Montana region ([Goldman et al., 2020](https://www.sciencebase.gov/catalog/item/5f1f058682cef313ed8e9e91)), run the following command:

```
poetry run python3 fusemine.py --tungsten
```
The evaluation provides group-wise accuracy and link-wise F1 scores, compared against a baseline method that uses only location data. 

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
