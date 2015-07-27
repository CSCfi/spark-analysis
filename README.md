[![Build Status](https://travis-ci.org/CSC-IT-Center-for-Science/spark-analysis.svg?branch=master)](https://travis-ci.org/CSC-IT-Center-for-Science/spark-analysis)

# Spark Analysis
Analyzing large datasets using spark 

# Directory Structure (Before running)
Place 'etc' and 'spar_jars' folders in /shared_data. Modify the config.yml.template in etc folder
Create 'mods' and 'tmp' empty directories in /shared_data

# Installation
python setup.py sdist to make the distribution.
pip install <foldername> to install (run from parent directory)

# Example Usage
from sparkles.modules.utils.runner import SparkRunner
sr = SparkRunner('/shared_data/etc/config.yml')
sr.import_dataset('/shared_data/files/FI4000047485_EUR.h5','some','some','swift://containerFiles.SparkTest')
sr.import_analysis('/shared_data/mods/','Liq', 'some','some', '/shared_data/github/spark-analysis/sparkles/modules/liq_curve_parquet.py','some','some','some')

params = {'tablename': 'ORDERS', 'start_time': 1349159961141, 'end_time': 1349160643981, 'interval': 60}
inputs = ['FI4000047485_EUR']
sr.run_analysis(modulename='Liq', params=params, inputs=inputs)


# Run dev scripts like
python -m bin.sparkles --config-file <e.g. etc/config.yml> [run, list, ...]

