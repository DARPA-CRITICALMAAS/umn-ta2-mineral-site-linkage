#!/bin/bash
#SBATCH --job-name=tungsten
#SBATCH --output=/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/tungsten.out
#SBATCH --time=24:00:00
#SBATCH -p a100-4    
#SBATCH --gres=gpu:a100:1 
#SBATCH --ntasks=1
#SBATCH --mem=40g
#SBATCH --mail-type=ALL
#SBATCH --mail-user=pyo00005@umn.edu

module load python3
source ~/.bashrc
conda activate ta2
cd /home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage
# python3 test.py -c cobalt
# python3 testing_euclidean_dist.py -c copper
# python3 test.py -c nickel
# python3 test.py -c tungsten
# python3 testing_cpu.py
# python3 tester.py
python3 evaluation_on_tungsten.py --commodity tungsten --intralink distance --interlink area
# cd /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn

# sleep 3h
# cp /home/yaoyi/pyo00005/CriticalMAAS/src/ta2-minmod-data/data/umn/sameas/tungsten_results.csv /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas/tungsten_sameas.csv
# git add --all
# git commit -m "Updated sameas for Tungsten"
# git push

# while [ ! -f /home/yaoyi/pyo00005/CriticalMAAS/src/ta2-minmod-data/data/umn/sameas/copper_results.csv ]
# do
#     sleep 1h
# done

# cp /home/yaoyi/pyo00005/CriticalMAAS/src/ta2-minmod-data/data/umn/sameas/copper_results.csv /home/yaoyi/pyo00005/CriticalMAAS/src/DATA/ta2-minmod-data/data/umn/sameas/copper_sameas.csv
# git add --all
# git commit -m "Updated sameas for Copper"
# git push


# python3 process_rawdata.py

# cd /home/yaoyi/pyo00005/CriticalMAAS/src/ta2-minmod-data
# git add --all
# git commit -m "MRDS data re"
# git push --set-upstream origin umn_MRDS_new
# python3 test.py
# python3 fusemine.py data_process -d /home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/full_MRDS.csv -m /home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/resource/sample_map_file.csv