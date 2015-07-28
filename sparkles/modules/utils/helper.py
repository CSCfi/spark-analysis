import h5py
from datetime import datetime, date, timedelta
from collections import defaultdict
import getpass
import re
from runner import SparkRunner
import yaml
import os
from os.path import dirname
import errno


# Hack for using HDF5 datasets in Spark, also fetches the data from dataset using the dates provided by user
def date_query(x, start_time, end_time):
    start = date.fromtimestamp(start_time)
    end = date.fromtimestamp(end_time)

    delta = timedelta(days=1)
    filepath = 'filepath here' + x
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

    with h5py.File(filepath) as f:
        data = f[str(x)].get(table)
        return list(data[:])


def saveDataset(configpath, dataframe, userdatadir, tablename, originalpath, description, details):

    p = re.compile('.+/(\w+)\.\w+')
    m = p.match(originalpath)
    filename = m.group(1)

    created = datetime.now()
    user = getpass.getuser()

    filedir = userdatadir + '/' + filename
    tablepath = filedir + '/' + filename + '_' + tablename + '.parquet'

    schema = str(dataframe.dtypes)
    params = defaultdict(str)
    params['name'] = filename
    params['fileformat'] = 'Parquet'
    params['created'] = created
    params['user'] = user

    params['description'] = description
    params['details'] = details

    params['filepath'] = filedir
    params['schema'] = schema

    if(tablename == "orders"):
        sr = SparkRunner(configpath)
        sr.create_dataset(params)

    # try:
    #    os.makedirs(filedir)
    # except OSError as exception:
    #    if exception.errno != errno.EEXIST:
    #        raise

    dataframe.saveAsParquetFile(tablepath)


def saveFeatures(configpath, dataframe, userdatadir, featureset_name, description, details, modulename, module_parameters, parent_datasets):

    filepath = userdatadir + '/' + featureset_name + ".parquet"
    created = datetime.now()
    user = getpass.getuser()

    schema = str(dataframe.dtypes)
    params = defaultdict(str)
    params['name'] = featureset_name
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

    sr = SparkRunner(configpath)
    sr.create_featureset(params)
    sr.create_relation(featureset_name, parent_datasets)
    dataframe.saveAsParquetFile(filepath)
