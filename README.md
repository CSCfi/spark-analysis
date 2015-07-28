[![Build Status](https://travis-ci.org/CSC-IT-Center-for-Science/spark-analysis.svg?branch=master)](https://travis-ci.org/CSC-IT-Center-for-Science/spark-analysis)

# Spark Analysis
Analyzing large datasets using spark 


# Installation
python setup.py sdist to make the distribution.
pip install ./<foldername> to install (run from parent directory)

#Directory Structures (Before Installation)
Place 'etc' and 'spark_jars' in the shared data folder of your cluster. Create tmp and mods directories in shared data folder itself. Edit the config.yml template in etc. Be sure to provide the read and write access to all the 4 directories.
Also make sure, the input files should be somewhere in shared data folder for all the spark workers to read

# Example Usage
```
from sparkles.modules.utils.runner import SparkRunner
sr = SparkRunner('path/to/config.yml')
sr.import_dataset(inputs='/path/to/filename.extension',userdatadir='swift://containerFiles.SparkTest',description='description of dataset',details='details')
sr.import_analysis(name='modulename', description='description', details='details', filepath='/path/to/module.py', params='params that are used in module', inputs='type of input it reads', outputs='kind of output it generates')

params = {'tablename': 'ORDERS', 'start_time': 1349159961141, 'end_time': 1349160643981, 'interval': 60}
inputs = ['filename']
sr.run_analysis(modulename='modulename', params=params, inputs=inputs)
```
If you need to generate a feature set out of the modules
```
features = {'userdatadir': 'swift://containerFeatures.SparkTest', 'description': 'something', 'details': 'something', 'modulename': 'module used to create this', 'featureset_name': 'featuresetname'}
params = {'tablename': 'ORDERS', 'start_time': 1349159961141, 'end_time': 1349160643981, 'interval': 60}
inputs = ['filename']

sr.run_analysis(modulename='modulename', params=params, inputs=inputs, features=features)
```
