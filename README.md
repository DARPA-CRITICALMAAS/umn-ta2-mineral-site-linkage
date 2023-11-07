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
