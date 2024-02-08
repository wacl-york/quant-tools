#!/bin/bash

#SBATCH --job-name=QUANT_CEDA_export_lcs
#SBATCH --mail-type=BEGIN,END,FAIL           
#SBATCH --mail-user=stuart.lacy@york.ac.uk   
#SBATCH --time=01:30:00                     
#SBATCH --account=chem-bocs-2020
#SBATCH --cpus-per-task=2
#SBATCH --ntasks=1
#SBATCH --mem-per-cpu=4G
#SBATCH --output=logs/output
#SBATCH --error=logs/error
#SBATCH --array=5,12,18,26,31,39,53,64,67,79,82,84,85,89,165,178

echo "Loaded script"
ml load R/4.3.2-gfbf-2023a
ml load PostgreSQL/16.1-GCCcore-12.3.0
echo "Loaded modules"
Rscript --vanilla export_lcs_worker_csv.R $SLURM_ARRAY_TASK_ID
echo "Ran job"
