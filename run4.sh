#!/bin/bash
#SBATCH --job-name=tungsten_mini
#SBATCH --output=/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/document_testing/testing_tungsten.out
#SBATCH --time=12:00:00
#SBATCH -p aglarge
#SBATCH --ntasks=1
#SBATCH --mem=10g
#SBATCH --mail-type=ALL
#SBATCH --mail-user=pyo00005@umn.edu

cd /home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/
module load python3
# python3 individual.py
python3 testing_mini_W.py
# python3 mineralsite.py