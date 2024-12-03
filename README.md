# FuseMine
<!-- Access the complete usage documentation in PDF format [here](https://github.com/DARPA-CRITICALMAAS/umn-ta2-mineral-site-linkage/blob/main/FuseMine%20Usage%20Documentation.pdf). -->

## Overview
FuseMine is a comprehensive pipeline designed to link data across multiple mineral site databases, such as USMIN and MRDS, and mineral site records extracted from reports and tables through tools created by TA2 Inferlink and USC. It facilitates the reconciliation process by utilizing both geospatial data and semantic similarities in textual attributes, such as site name and commodity. 

## Requirements
To run FuseMine, ensure the following requirements are met:
- **Hardware**: A local device with GPU support is recommended.
- **Software**: Docker must be installed. Follow the installation steps on the official [Docker website](https://docs.docker.com/engine/install/).
- **Memory**: At least 10 GB of available memory is suggested (actual requirements depend on the data size).
- **Network**: An active internet connection is required during execution.

FuseMine simplifies usage by providing shell scripts for common workflows, including processing raw data into schema format and linking mineral sites. These scripts:
1. Clone the [MinMod data repository](https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data) (if not already cloned).
2. Create and run a Docker container for the specified task.
3. Export the resulting data from the container
4. Push the data to the data repository.

Ensure you are running these scripts on a system capable of executing Bash scripts.

## Installation
1. Clone the repository
```
git clone https://github.com/DARPA-CRITICALMAAS/umn-ta2-mineral-site-linkage.git
cd umn-ta2-mineral-site-linkage
```

## Usage
### Data Processing
The data processing step requires a manually curated attribute map file, which maps the headers of the raw structured data to the MinMod KG schema. An example of an attribute map CSV is provided: [sample_mapfile.csv](https://github.com/DARPA-CRITICALMAAS/umn-ta2-mineral-site-linkage/blob/main/sample_mapfile.csv). Attributes that FuseMine can map to are listed under 'attribute_label'. If there is no label in the raw data representing certain label (e.g., there is no column representing 'deposit_type_candidate') leave the 'corresponding_attribute_label' empty.

#### Command:
```
./run_processing.sh <path_to_raw_data> <path_to_attribute_map>
```

#### Arguments:
- `path_to_raw_data`: Path (either file or directory) to the raw mineral site database.
- `path_to_attribute_map`: Path to the CSV file containing label mapping information (refer to [sample_mapfile.csv](https://github.com/DARPA-CRITICALMAAS/umn-ta2-mineral-site-linkage/blob/main/sample_mapfile.csv) for guidance)

*Processing Time*: Several hours depending on the size of the raw data.

### Data Reconciliation
Before running FuseMine, ensure that all data is available on the [MinMod Knowledge Graph](https://minmod.isi.edu/) (MinMod KG). The last update time and version of MinMod KG can be checked [here](https://minmod.isi.edu/resource/kg).

#### Command:
```
./run_linking.sh <commodity>
```

#### Arguments:
- `commodity`: The specific commodity to focus on. The commodity name must match one of the MRDS commodity names listed [here](https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data/blob/main/data/entities/commodity.csv).

FuseMine generates a log file that reports the number of available records on the MinMod KG, the number of identified mineral sites, and the run time. These log files are saved in the `./logs` directory.

Once the data is merged into the main branch, it typically takes 5 to 10 minutes for the changes to be reflected on the KG.