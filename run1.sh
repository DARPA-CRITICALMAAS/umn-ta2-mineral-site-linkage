#!/bin/bash
#SBATCH --job-name=umap_reduction
#SBATCH --output=/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/run_results/embedding_reduction.out
#SBATCH --time=8:00:00
#SBATCH -p k40                                             
#SBATCH --gres=gpu:k40:1
#SBATCH --ntasks=1
#SBATCH --mem=10g
#SBATCH --mail-type=END
#SBATCH --mail-user=pyo00005@umn.edu

cd /home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/
module load python3
python3 -m minelink.mid_run -d /home/yaoyi/pyo00005/CriticalMAAS/src/data/raw