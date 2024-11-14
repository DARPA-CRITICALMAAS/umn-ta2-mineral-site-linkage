#!/bin/bash
#SBATCH --job-name=copper2
#SBATCH --output=/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/copper2.out
#SBATCH --time=5:00:00
#SBATCH -p interactive-gpu
#SBATCH --gres=gpu:a40:1
#SBATCH --ntasks=1
#SBATCH --mem=80g
#SBATCH --mail-type=ALL
#SBATCH --mail-user=pyo00005@umn.edu

module load python3
source ~/.bashrc

cd /home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage

# conda activate ta2

# poetry run python3 

# cd /home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/
# python3 utils/load_kg_data.py
# conda activate fusemine

# conda remove -n test --all
# conda create -n test python=3.10
# conda activate test
# pip install -r requirements.txt
# pip3 install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118

# python3 fusemine.py --commodity cerium --intralink distance --interlink area
# python3 gen.py

# python3 mini.py

# python3 fusemine.py --commodity yttrium --intralink distance --interlink area --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename yttrium_sameas_0826
# python3 fusemine.py --commodity cerium --intralink distance --interlink area  --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename cerium_sameas_0826
# python3 fusemine.py --commodity dysprosium --intralink distance --interlink area --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename dysprosium_sameas_0826
# python3 fusemine.py --commodity erbium --intralink distance --interlink area --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename erbium_sameas_0826
# python3 fusemine.py --commodity europium --intralink distance --interlink area --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename europium_sameas_0826
# python3 fusemine.py --commodity gadolinium --intralink distance --interlink area --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename gadolinium_sameas_0826
# python3 fusemine.py --commodity holmium --intralink distance --interlink area --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename holmium_sameas_0826
# python3 fusemine.py --commodity lanthanum --intralink distance --interlink area --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename lanthanum_sameas_0826
# python3 fusemine.py --commodity niobium --intralink distance --interlink area --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename niobium_sameas_0826
# python3 fusemine.py --commodity lutetium --intralink distance --interlink area --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename lutetium_sameas_0826
# python3 fusemine.py --commodity praseodymium --intralink distance --interlink area --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename praseodymium_sameas_0826
# python3 fusemine.py --commodity samarium --intralink distance --interlink area --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename samarium_sameas_0826
# python3 fusemine.py --commodity tantalum --intralink distance --interlink area --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename tantalum_sameas_0826
# python3 fusemine.py --commodity terbium --intralink distance --interlink area --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename terbium_sameas_0826
# python3 fusemine.py --commodity thulium --intralink distance --interlink area --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename thulium_sameas_0826
# python3 fusemine.py --commodity ytterbium --intralink distance --interlink area --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename ytterbium_sameas_0826

# python3 fusemine.py --commodity zinc --intralink distance --interlink area --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename zinc_sameas_0826
# python3 fusemine.py --commodity copper --intralink distance --interlink area --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename copper_sameas_0826
# python3 fusemine.py --commodity tungsten --intralink distance --interlink area --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename tungsten_sameas_0826

# python3 fusemine.py --commodity nickel --intralink distance --interlink area  --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename nickel_sameas_0826
# python3 fusemine.py --commodity lithium --intralink distance --interlink area  --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename lithium_sameas_0826
# python3 fusemine.py --commodity cobalt --intralink distance --interlink area  --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename cobalt_sameas_0826

# python3 fusemine.py --commodity gallium --intralink distance --interlink area --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename gallium_sameas_0826
# python3 fusemine.py --commodity germanium --intralink distance --interlink area --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename germanium_sameas_0826
# python3 fusemine.py --commodity indium -intralink distance --interlink area --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename indium_sameas_0826
# # python3 fusemine.py --commodity silver --intralink distance --interlink area --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename silver_sameas_0826
# python3 fusemine.py --commodity lead --intralink distance --interlink area --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename lead_sameas_0826
# python3 fusemine.py --commodity molybdenum --intralink distance --interlink area --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename molybdenum_sameas_0826
# python3 fusemine.py --commodity rhenium --intralink distance --interlink area --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename rhenium_sameas_0826
# python3 fusemine.py --commodity tellurium --intralink distance --interlink area --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename tellurium_sameas_0826
# # python3 fusemine.py --commodity gold --intralink distance --interlink area --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename gold_sameas_0826
# python3 fusemine.py --commodity "platinum-group elements" --intralink distance --interlink area --same_as_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas --same_as_filename pge_sameas_0826

poetry run python3 fusemine.py --commodity nickel --intralink distance --interlink area
# python3 fusemine.py --tungsten

# python3 testing.py

# python3 fusemine.py --tungsten
# /home/yaoyi/pyo00005/.conda/envs/fusemine/bin/pip install -r requirements.txt
# /home/yaoyi/pyo00005/.conda/envs/pip install strsim
# python --version
# pip --version

# which python
# which /home/yaoyi/pyo00005/.conda/envs/ta2/bin/pip
# /home/yaoyi/pyo00005/.conda/envs/ta2/bin/pip list
# pip list
# python3 process_data_to_schema.py --raw_data /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/RAW_DATA/https:__mrdata.usgs.gov_mrds/mrds.csv --attribute_map /home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/sample_mapfile.csv --schema_output_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn --schema_output_filename "mrds"
# python3 process_data_to_schema.py --raw_data /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/RAW_DATA/https:__mrdata.usgs.gov_major_deposits/deposit.csv --attribute_map /home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/sample_mapfile2.csv --schema_output_directory /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn --schema_output_filename major_deposits

# python3 fusemine.py --subset_file /home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/base_metal.xls --subset_map /home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/subset_map.csv --kg_file /home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/kg_zinc.csv 