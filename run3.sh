#!/bin/bash
#SBATCH --job-name=MRDS_MVT
#SBATCH --output=/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/run_results/MRDS_MVT.out
#SBATCH --time=1:00:00
#SBATCH -p a100-8
#SBATCH --gres=gpu:a100:1
#SBATCH --ntasks=1
#SBATCH --mem=10g
#SBATCH --mail-type=ALL
#SBATCH --mail-user=pyo00005@umn.edu

cd /home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/
module load mamba
module load python3
conda activate ta2
python3 sample.py