#!/bin/bash
#SBATCH --job-name=individual_text_test
#SBATCH --output=/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/document_testing/individual_text.out
#SBATCH --time=24:00:00
#SBATCH -p k40                                             
#SBATCH --gres=gpu:k40:1
#SBATCH --ntasks=1
#SBATCH --mem=10g
#SBATCH --mail-type=ALL
#SBATCH --mail-user=pyo00005@umn.edu

cd /home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/
module load python3
python3 individual_parts_ver.py