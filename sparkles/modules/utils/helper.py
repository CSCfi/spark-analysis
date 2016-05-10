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
import socket
from snakebite.client import Client
import subprocess


def import_hdf5(x, filepath, table):

    with h5py.File(filepath) as f:
        data = f[str(x)].get(table)
        return list(data[:])


def saveDataset(configstr, dataframe, userdatadir, tablename, originalpath, description, details):

    p = re.compile('.+/(\w+)\.\w+')
    m = p.match(originalpath)
    identifier = m.group(1)

    created = datetime.now()
    user = getpass.getuser()

    filedir = userdatadir + identifier  # This assumes you already have a trailing forward slash in the userdatadir parameter
    filename = identifier + '_' + tablename.upper()
    tablepath = filedir + '/' + filename + '.parquet'

    schema = str(dataframe.dtypes)
    params = defaultdict(str)
    params['name'] = filename
    params['identifier'] = identifier
    params['fileformat'] = 'Parquet'
    params['created'] = created
    params['user'] = user

    params['description'] = description
    params['details'] = details

    params['filepath'] = tablepath
    params['schema'] = schema

    try:
        dataframe.write.parquet(tablepath)
    except Exception as e:
        raise RuntimeError(e)

    sessionconfig = config_session(configstr)
    create_dataset(sessionconfig, params)


def saveFeatures(dataframe, features, module_parameters, inputs):

    parent_datasets = []
    for input_item in inputs:
        input_item_filepath = str(input_item)
        input_item_filename, file_extension = os.path.splitext(os.path.basename(os.path.abspath(input_item_filepath)))
        parent_datasets.append(input_item_filename)  # Just append the names of the dataset used not the full path (Fetched from metadata)

    userdatadir = str(features['userdatadir'])
    description = str(features['description'])
    details = str(features['details'])
    featureset_name = str(features['featureset_name'])
    modulename = str(features['modulename'])

    configstr = str(features['configstr'])

    filepath = userdatadir + featureset_name + ".parquet"  # This assumes you already have a trailing forward slash in the userdatadir parameter
    created = datetime.now()
    user = getpass.getuser()

    schema = str(dataframe.dtypes)
    params = defaultdict(str)
    params['name'] = featureset_name
    params['identifier'] = ''
    params['fileformat'] = 'Parquet'
    params['created'] = created
    params['user'] = user

    params['description'] = description
    params['details'] = details
    params['modulename'] = modulename
    params['module_parameters'] = json.dumps(module_parameters)
    params['parents'] = json.dumps(parent_datasets)

    params['filepath'] = filepath
    params['schema'] = schema

    try:
        dataframe.write.parquet(filepath)
    except Exception as e:
        raise RuntimeError(e)

    sessionconfig = config_session(configstr)
    create_featureset(sessionconfig, params)
    create_relation(sessionconfig, featureset_name, parent_datasets)


def config_session(configstr):

    config = json.loads(configstr)
    dburi = config['METADATA_URI']
    session = config_to_db_session(dburi, Base)
    return (session, config)


def getObjsBackend(objs, backend, config):

    if(backend == 'hdfs'):

        client = Client(socket.gethostname(), config['HADOOP_RPC_PORT'], use_trash=False)

        for obj in objs:
                try:
                    copy_gen = client.copyToLocal([obj[0]], obj[1])
                    for copy_item in copy_gen:
                        pass
                except Exception as e:
                        print(e)
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

    elif(backend == 'nfs'):  # Every file is already in respective local dirs
        pass


def saveObjsBackend(objs, backend, config):

    if(backend == 'hdfs'):
        for obj in objs:
            try:
                # obj[0] is hdfs path and obj[1] is local filesystem path
                subprocess.check_call(['hdfs', 'dfs', '-copyFromLocal', '-f', obj[1], obj[0]])
            except Exception as e:
                shutil.copyfile(config['BACKUP_METADATA_LOCAL_PATH'], config['METADATA_LOCAL_PATH'])
                raise RuntimeError(e)

    elif(backend == 'swift'):
        options = {'os_auth_url': os.environ['OS_AUTH_URL'], 'os_username': os.environ['OS_USERNAME'], 'os_password': os.environ['OS_PASSWORD'], 'os_tenant_id': os.environ['OS_TENANT_ID'], 'os_tenant_name': os  .environ['OS_TENANT_NAME']}
        swiftService = SwiftService(options=options)
        objects = []
        for obj in objs:
            objects.append(SwiftUploadObject(obj[1], object_name=obj[0]))

            swiftUpload = swiftService.upload(container='containerModules', objects=objects)
            for uploaded in swiftUpload:
                if("error" in uploaded.keys()):
                    shutil.copyfile(config['BACKUP_METADATA_LOCAL_PATH'], config['METADATA_LOCAL_PATH'])
                    raise RuntimeError(uploaded['error'])

    elif(backend == 'nfs'):
        for obj in objs:
            shutil.copyfile(obj[1], config['MODULES_DIR'] + obj[0])

    print('Metadata/Module changed and uploaded')


def create_dataset(sessionconfig, params):

    session = sessionconfig[0]
    config = sessionconfig[1]

    checkDataset = session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
        params(name=params['name']).first()

    if(checkDataset is None):

        dataset = Dataset(name=params['name'], identifier=params['identifier'], description=params['description'], details=params['details'], module_parameters='', created=params['created'], user=params['user'], fileformat="Parquet", filepath=params['filepath'], schema=params['schema'], module_id='')
        shutil.copyfile(config['METADATA_LOCAL_PATH'], config['BACKUP_METADATA_LOCAL_PATH'])

        session.add(dataset)
        session.commit()

        objs = []
        if(config['BACKEND'] == 'hdfs'):
            objs.append((config['MODULES_DIR'] + 'sqlite.db', config['METADATA_LOCAL_PATH']))
        elif(config['BACKEND'] == 'swift'):
            objs.append(('sqlite.db', config['METADATA_LOCAL_PATH']))
        elif(config['BACKEND'] == 'nfs'):
            pass

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

        # module_id = analysisMod.id
        checkDataset = session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
            params(name=params['name']).first()

        if(checkDataset is None):
            dataset = Dataset(name=params['name'], identifier='', description=params['description'], details=params['details'], module_parameters=params['module_parameters'], created=params['created'], user=params['user'], fileformat="Parquet", filepath=params['filepath'], schema=params['schema'], module_id=analysisMod.id)
            shutil.copyfile(config['METADATA_LOCAL_PATH'], config['BACKUP_METADATA_LOCAL_PATH'])

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
    for parent in parents:
        dataset = session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
            params(name=parent).first()
        f = fs_to_ds.insert().values(left_fs_id=featureset.id, right_ds_id=dataset.id)
        session.execute(f)

    session.commit()
    objs = []
    if(config['BACKEND'] == 'hdfs'):
        objs.append((config['MODULES_DIR'] + 'sqlite.db', config['METADATA_LOCAL_PATH']))
    elif(config['BACKEND'] == 'swift'):
        objs.append(('sqlite.db', config['METADATA_LOCAL_PATH']))
    elif(config['BACKEND'] == 'nfs'):
        pass

    saveObjsBackend(objs, config['BACKEND'], config)


def delete_item(config, filepath='', localpath=''):

    if(config['BACKEND'] == 'hdfs'):
        client = Client(socket.gethostname(), config['HADOOP_RPC_PORT'], use_trash=False)
        del_gen = client.delete([filepath], recurse=True)
        for del_item in del_gen:
            pass
    elif(config['BACKEND'] == 'swift'):
        pass  # To be implemented

    # Deleting modules or datasets from local directories (will also suffice for nfs backend)
    if(os.path.isdir(localpath)):  # Check if it is a dataset
        shutil.rmtree(localpath)
    else:
        try:
            os.remove(localpath)
        except OSError:
            pass
