import h5py
from datetime import datetime, date, timedelta
from collections import defaultdict
import getpass
import re
from sparkles.runner import SparkRunner
import yaml


# Hack for using HDF5 datasets in Spark, also fetches the data from dataset using the dates provided by user
def date_query(x, start_time, end_time):
    start = date.fromtimestamp(start_time)
    end = date.fromtimestamp(end_time)

    delta = timedelta(days=1)
    filepath = '//shared_data//files//' + x
    with h5py.File(filepath) as curr_file:
        res = []
        while start <= end:
            currdate = start.strftime("%Y_%m_%d")
            if currdate in curr_file:
                dategrp = curr_file[currdate]
                datedata = dategrp.get('ORDERS')
                res.append(list(datedata[:]))
            start += delta
        return sum(res, [])


def import_hdf5(x, filepath, table):

    # filepath = 'files/' + filepath
    with h5py.File(filepath) as f:
        data = f[str(x)].get(table)
        return list(data[:])


def saveDataset(dataframe, filepath, description, details):

    p = re.compile('.+/(\w+)\.\w+')
    m = p.match(filepath)
    filename = m.group(1)

    created = datetime.now()
    user = getpass.getuser()

    schema = str(dataframe.dtypes)
    params = defaultdict(str)
    params['name'] = filename
    params['fileformat'] = 'Parquet'
    params['created'] = created
    params['user'] = user

    params['description'] = description
    params['details'] = details

    params['filepath'] = filepath
    params['schema'] = schema

    config = yaml.load("/shared_data/github/spark_analysis/etc/config.yml")
    sr = SparkRunner(config)
    sr.create_dataset(params)
    dataframe.saveAsParquetFile(filepath)


def saveFeatures(dataframe, filename, description, details, modulename, module_parameters, parent_datasets):

    filepath = "/shared_data/files/" + filename + ".parquet"
    created = datetime.now()
    user = getpass.getuser()

    schema = str(dataframe.dtypes)
    params = defaultdict(str)
    params['name'] = filename
    params['fileformat'] = 'Parquet'
    params['created'] = created
    params['user'] = user

    params['description'] = description
    params['details'] = details
    params['modulename'] = modulename
    params['module_parameters'] = module_parameters
    params['parents'] = parent_datasets

    params['filepath'] = filepath
    params['schema'] = schema

    config = yaml.load("/shared_data/github/spark_analysis/etc/config.yml")
    sr = SparkRunner(config)
    sr.create_featureset(params)
    sr.create_relation(filename, parent_datasets)
    dataframe.saveAsParquetFile(filepath)
