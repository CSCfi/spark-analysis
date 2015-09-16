import h5py
from datetime import datetime, date, timedelta
from collections import defaultdict
import getpass
import re
import yaml
import os
from os.path import dirname
import errno
from models import Base, config_to_db_session, fs_to_ds, Dataset, Analysis
from sqlalchemy import text
from swiftclient.service import SwiftService, SwiftUploadObject
import shutil
import json
import hdfs
from hdfs import InsecureClient
import socket


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

    try:
        dataframe.saveAsParquetFile(tablepath)
    except Exception as e:
        raise RuntimeError(e)

    if(tablename == "orders"):
        sessionconfig = config_session(configpath)
        create_dataset(sessionconfig, params)


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

    try:
        dataframe.saveAsParquetFile(filepath)
    except Exception as e:
        raise RuntimeError(e)

    sessionconfig = config_session(configpath)
    create_featureset(sessionconfig, params)
    create_relation(sessionconfig, featureset_name, parent_datasets)


def config_session(configpath):

    config = None
    with open(configpath, 'r') as config_file:
        config = yaml.load(config_file)

    dburi = config['DATABASE_URI']
    session = config_to_db_session(dburi, Base)
    return (session, config)


def getObjsBackend(objs, backend):

    if(backend == 'hdfs'):

        client = InsecureClient('http://' + socket.gethostname() + ':50070')

        for obj in objs:
            retry = 0
            while(retry < 10):
                try:
                    client.download(obj[0], obj[1], overwrite=False)
                except hdfs.util.HdfsError as e:
                    print(str(e))
                    if(len(str(e)) < 1000):
                        break
                    if(retry == 10 and len(str(e)) > 1000):
                        raise RuntimeError(e)
                retry = retry + 1
    elif(backend == 'swift'):

        options = {'os_auth_url': os.environ['OS_AUTH_URL'], 'os_username': os.environ['OS_USERNAME'], 'os_password': os.environ['OS_PASSWORD'], 'os_tenant_id': os.environ['OS_TENANT_ID'], 'os_tenant_name': os.environ['OS_TENANT_NAME']}
        swiftService = SwiftService(options=options)

        for obj in objs:

            # Create the containers which are used in this application for Object Storage
            if(obj[0] == 'sqlite.db'):
                swiftService.post(container='containerFiles')
                swiftService.post(container='containerFeatures')
                swiftService.post(container='containerModules')

            out_file = obj[1]  # Get the output file location from runner
            localoptions = {'out_file': out_file}
            objects = []
            objects.append(obj[0])
            swiftDownload = swiftService.download(container='containerModules', objects=objects, options=localoptions)

            for downloaded in swiftDownload:
                if("error" in downloaded.keys()):
                    raise RuntimeError(downloaded['error'])
                # print(downloaded)


def saveObjsBackend(objs, backend, config):

    if(backend == 'hdfs'):
        client = InsecureClient('http://' + socket.gethostname() + ':50070')
        client.makedirs('/modules')
        for obj in objs:
            try:
                client.upload(obj[0], obj[1], overwrite=True)
            except Exception as e:
                shutil.copyfile('/shared_data/sparkles/tmp/sqlite_temp.db', config['DB_LOCATION'])
                raise RuntimeError(e)

    elif(backend == 'swift'):
        swiftService = SwiftService(options=options)
        objects = []
        for obj in objs:
            objects.append(SwiftUploadObject(obj[1], object_name=obj[0]))

            swiftUpload = swiftService.upload(container='containerModules', objects=objects)
            for uploaded in swiftUpload:
                if("error" in uploaded.keys()):
                    shutil.copyfile('/shared_data/sparkles/tmp/sqlite_temp.db', config['DB_LOCATION'])
                    raise RuntimeError(uploaded['error'])
    print('Metadata/Module changed and uploaded')


def create_dataset(sessionconfig, params):

    session = sessionconfig[0]
    config = sessionconfig[1]

    checkDataset = session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
        params(name=params['name']).first()

    if(checkDataset is None):

        dataset = Dataset(name=params['name'], description=params['description'], details=params['details'], module_parameters='', created=params['created'], user=params['user'], fileformat="Parquet", filepath=params['filepath'], schema=params['schema'], module_id='')
        shutil.copyfile(config['DB_LOCATION'], '/shared_data/sparkles/tmp/sqlite_temp.db')

        session.add(dataset)
        session.commit()

        objs = []
        if(config['BACKEND'] == 'hdfs'):
            objs.append(('/modules/sqlite.db', config['DB_LOCATION']))
        elif(config['BACKEND'] == 'swift'):
            objs.append(('sqlite.db', config['DB_LOCATION']))

        saveObjsBackend(objs, config['BACKEND'], config)

    else:
        raise RuntimeError("The dataset with name " + params['name'] + " already exists")


def create_featureset(sessionconfig, params):

    session = sessionconfig[0]
    config = sessionconfig[1]

    modulename = params['modulename']
    analysisMod = session.query(Analysis).from_statement(text("SELECT * FROM analysis where name=:name")).\
        params(name=modulename).first()

    if(analysisMod):  # Check if the module exists

        module_id = analysisMod.id
        checkDataset = session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
            params(name=params['name']).first()

        if(checkDataset is None):
            dataset = Dataset(name=params['name'], description=params['description'], details=params['details'], module_parameters=params['module_parameters'], created=params['created'], user=params['user'], fileformat="Parquet", filepath=params['filepath'], schema=params['schema'], module_id=analysisMod.id)
            shutil.copyfile(config['DB_LOCATION'], '/shared_data/sparkles/tmp/sqlite_temp.db')

            session.add(dataset)
            session.commit()

        else:
            raise RuntimeError('The feature set with the name ' + params['name'] + ' already exists')
    else:
        raise RuntimeError('No Such Module')


def create_relation(sessionconfig, featset, parents):

    session = sessionconfig[0]
    config = sessionconfig[1]

    featureset = session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
        params(name=featset).first()
    parents = json.loads(parents)
    for parent in parents:
        dataset = session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
            params(name=parent).first()
        f = fs_to_ds.insert().values(left_fs_id=featureset.id, right_ds_id=dataset.id)
        session.execute(f)

    session.commit()
    objs = []
    if(config['BACKEND'] == 'hdfs'):
        objs.append(('/modules/sqlite.db', config['DB_LOCATION']))
    elif(config['BACKEND'] == 'swift'):
        objs.append(('sqlite.db', config['DB_LOCATION']))

    saveObjsBackend(objs, config['BACKEND'], config)
