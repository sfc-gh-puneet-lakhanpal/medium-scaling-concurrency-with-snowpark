#!/bin/bash
# ------------------------------------------
# -- Activate conda environment
conda activate myenvironment
#
# echo "conda info: $(conda info)"
# -- Read CSV
SECONDS=0
cat config.csv | parallel --jobs 100 --progress --colsep , " python job.py –contract-type {1} --start-date {2} --end-date {3} | tee {1}_{2}_{3}.out "
duration=$SECONDS
echo "$(($duration/60)) minutes and $(($duration % 60)) seconds elapsed."