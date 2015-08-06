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
from models import Base, config_to_db_session, fs_to_ds, Dataset, Analysis
from sqlalchemy import text
from swiftclient.service import *


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
        sessionconfig = config_session(configpath)
        create_dataset(sessionconfig, params)

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

    sessionconfig = config_session(configpath)
    create_featureset(sessionconfig, params)
    create_relation(sessionconfig, featureset_name, parent_datasets)
    dataframe.saveAsParquetFile(filepath)


def config_session(configpath):

    config = None
    with open(configpath, 'r') as config_file:
        config = yaml.load(config_file)

    dburi = config['DATABASE_URI']
    session = config_to_db_session(dburi, Base)
    return (session, config)


def create_dataset(sessionconfig, params):

    session = sessionconfig[0]
    config = sessionconfig[1]

    checkDataset = session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
        params(name=params['name']).first()

    if(checkDataset is None):

        dataset = Dataset(name=params['name'], description=params['description'], details=params['details'], module_parameters='', created=params['created'], user=params['user'], fileformat="Parquet", filepath=params['filepath'], schema=params['schema'], module_id='')
        session.add(dataset)
        session.commit()

        options = {'os_auth_url': config['SWIFT_AUTH_URL'], 'os_username': config['SWIFT_USERNAME'], 'os_password': config['SWIFT_PASSWORD'], 'os_tenant_id': config['SWIFT_TENANT_ID'], 'os_tenant_name': config['SWIFT_TENANT_NAME']}
        swiftService = SwiftService(options=options)
        objects = []
        objects.append(SwiftUploadObject(config['DB_LOCATION'], object_name='sqlite.db'))

        swiftUpload = swiftService.upload(container='containerModules', objects=objects)
        for uploaded in swiftUpload:
            print("Metadata changed and uploaded")

    else:
        raise ValueError("The dataset with name " + params['name'] + " already exists")


def create_featureset(sessionconfig, params):

    session = sessionconfig[0]
    config = sessionconfig[1]

    modulename = params['modulename']
    analysisMod = session.query(Analysis).from_statement(text("SELECT * FROM analysis where name=:name")).\
        params(name=modulename).first()

    if(not analysisMod):  # Check if the module exists

        module_id = analysisMod.id
        checkDataset = session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
            params(name=params['name']).first()

        if(checkDataset is None):
            dataset = Dataset(name=params['name'], description=params['description'], details=params['details'], module_parameters=params['module_parameters'], created=params['created'], user=params['user'], fileformat="Parquet", filepath=params['filepath'], schema=params['schema'], module_id=analysisMod.id)
            session.add(dataset)
            session.commit()

            options = {'os_auth_url': config['SWIFT_AUTH_URL'], 'os_username': config['SWIFT_USERNAME'], 'os_password': config['SWIFT_PASSWORD'], 'os_tenant_id': config['SWIFT_TENANT_ID'], 'os_tenant_name': config['SWIFT_TENANT_NAME']}
            swiftService = SwiftService(options=options)
            objects = []
            objects.append(SwiftUploadObject(config['DB_LOCATION'], object_name='sqlite.db'))
            swiftUpload = swiftService.upload(container='containerModules', objects=objects)
            for uploaded in swiftUpload:
                print("Metadata changed , uploaded")
        else:
            raise ValueError('The feature set with the name ' + params['name'] + ' already exists')
    else:
        raise ValueError('No Such Module')


def create_relation(sessionconfig, featset, parents):

    session = sessionconfig[0]
    featureset = session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
        params(name=featset).first()
    parents = json.loads(parents)
    for parent in parents:
        dataset = session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
            params(name=parent).first()
        f = fs_to_ds.insert().values(left_fs_id=featureset.id, right_ds_id=dataset.id)
        session.execute(f)

    session.commit()
