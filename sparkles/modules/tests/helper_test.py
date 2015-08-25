from mock import Mock, patch, mock_open
import sparkles.modules.utils.models as SparklesModels
from sparkles.modules.utils.models import Base, Dataset, Analysis, config_to_db_session
from side_effects import mod_se, ds_se, feat_se, relation_se, call_se, list_ds_se, list_mod_se

import h5py
from datetime import datetime, date, timedelta
from collections import defaultdict
import getpass
import re
import yaml
import os
from os.path import dirname
import errno
from sparkles.modules.utils.models import Base, config_to_db_session, fs_to_ds, Dataset, Analysis
from sqlalchemy import text
from swiftclient.service import *
import shutil


def saveDataset(configpath, dataframe, userdatadir, tablename, originalpath, description, details):

    p = re.compile('.+/(\w+)\.\w+')
    m = p.match(originalpath)
    filename = m.group(1)

    created = datetime.now()
    user = 'root'

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

    return 1


def saveFeatures(configpath, dataframe, userdatadir, featureset_name, description, details, modulename, module_parameters, parent_datasets):

    filepath = userdatadir + '/' + featureset_name + ".parquet"
    created = datetime.now()
    user = 'root'

    print(filepath)
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
    return 1


def config_session(configpath):

    config = None
    yamlmock = Mock(spec=yaml)
    with patch('__main__.open', mock_open(read_data={})) as m:
        with open(configpath) as config_file:
            config = yamlmock.load(config_file).return_value = {'DATABASE_URI': '', 'DB_LOCATION': '', 'CLUSTER_URL': '', 'MODULE_LOCAL_STORAGE': '', 'SWIFT_AUTH_URL': '', 'SWIFT_USERNAME': '', 'SWIFT_PASSWORD': '', 'SWIFT_TENANT_ID': '', 'SWIFT_PASSWORD': '', 'SWIFT_TENANT_NAME': ''}

    sparklesModels = Mock(spec=SparklesModels)
    dburi = config['DATABASE_URI']
    session = sparklesModels.config_to_db_session(dburi, Base)
    return (session, config)


def create_dataset(sessionconfig, params):

    session = sessionconfig[0]
    config = sessionconfig[1]

    sessionDsQuery = session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
        params(name=params['name']).first().side_effect = ds_se
    checkDataset = sessionDsQuery(name=params['name'])

    if(checkDataset is None):

        dataset = Dataset(name=params['name'], description=params['description'], details=params['details'], module_parameters='', created=params['created'], user=params['user'], fileformat="Parquet", filepath=params['filepath'], schema=params['schema'], module_id='')

        shutilmock = Mock(spec=shutil)
        shutilmock.copyfile(config['DB_LOCATION'], '/shared_data/sparkles/tmp/sqlite_temp.db')

        session.add(dataset)
        session.commit()

        options = {'os_auth_url': config['SWIFT_AUTH_URL'], 'os_username': config['SWIFT_USERNAME'], 'os_password': config['SWIFT_PASSWORD'], 'os_tenant_id': config['SWIFT_TENANT_ID'], 'os_tenant_name': config['SWIFT_TENANT_NAME']}

        swiftService = Mock(spec=SwiftService(options=options))
        objects = []
        objects.append(SwiftUploadObject(config['DB_LOCATION'], object_name='sqlite.db'))

        swiftUpload = swiftService.upload(container='containerModules', objects=objects).return_value = ({'success': 'true'}, {})
        for uploaded in swiftUpload:
            if("error" in uploaded.keys()):
                shutilmock = Mock(spec=shutil)
                shutilmock.copyfile('/shared_data/sparkles/tmp/sqlite_temp.db', config['DB_LOCATION'])
                raise RuntimeError(uploaded['error'])
            print(uploaded)

    else:
        raise RuntimeError("The dataset with name " + params['name'] + " already exists")


def create_featureset(sessionconfig, params):

    session = sessionconfig[0]
    config = sessionconfig[1]

    modulename = params['modulename']

    sessionQuery = session.query(Analysis).from_statement(text("SELECT * FROM analysis where name=:name")).\
        params(name=modulename).first().side_effect = mod_se
    analysisMod = sessionQuery(name=modulename)

    if(analysisMod):  # Check if the module exists

        module_id = analysisMod.id
        featureQuery = session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
            params(name=params['name']).first().side_effect = feat_se
        checkDataset = featureQuery(name=params['name'])

        if(checkDataset is None):
            dataset = Mock(spec=Dataset(name=params['name'], description=params['description'], details=params['details'], module_parameters=params['module_parameters'], created=params['created'], user=params['user'], fileformat="Parquet", filepath=params['filepath'], schema=params['schema'], module_id=analysisMod.id))

            shutilmock = Mock(spec=shutil)
            shutilmock.copyfile(config['DB_LOCATION'], '/shared_data/sparkles/tmp/sqlite_temp.db')

            session.add(dataset)
            session.commit()

        else:
            raise RuntimeError('The feature set with the name ' + params['name'] + ' already exists')
    else:
        raise RuntimeError('No Such Module')


def create_relation(sessionconfig, featset, parents):

    session = sessionconfig[0]
    config = sessionconfig[1]

    featureQuery = session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
        params(name=featset).first().side_effect = relation_se
    featureset = featureQuery(name=featset)

    parents = json.loads(parents)
    for parent in parents:
        datasetQuery = session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
            params(name=parent).first().side_effect = ds_se
        dataset = datasetQuery(name=parent)

        f = fs_to_ds.insert().values(left_fs_id=featureset.id, right_ds_id=dataset.id)
        session.execute(f)

    session.commit()

    options = {'os_auth_url': config['SWIFT_AUTH_URL'], 'os_username': config['SWIFT_USERNAME'], 'os_password': config['SWIFT_PASSWORD'], 'os_tenant_id': config['SWIFT_TENANT_ID'], 'os_tenant_name': config['SWIFT_TENANT_NAME']}
    swiftService = Mock(spec=SwiftService(options=options))
    objects = []
    objects.append(SwiftUploadObject(config['DB_LOCATION'], object_name='sqlite.db'))
    swiftUpload = swiftService.upload(container='containerModules', objects=objects).return_value = ({'success': 'true'}, {})
    for uploaded in swiftUpload:
        if("error" in uploaded.keys()):
            shutilmock.copyfile('/shared_data/sparkles/tmp/sqlite_temp.db', config['DB_LOCATION'])
            raise RuntimeError(uploaded['error'])
        print(uploaded)

# from dataframe_mock import DataframeMock
# dataframe = DataframeMock()
# userdatadir = 'swift://containerFiles.SparkTest'
# saveDataset('/path/to/config.yml', dataframe, userdatadir, "orders", '/path/to/file.h5', 'description', 'details')
# userdatadir = 'swift://containerFeatures.SparkTest'
# featureset_name = 'feat'
# modulename = 'existing'
# module_parameters = {}
# parent_datasets = ['existing_dataset']
# saveFeatures('/path/to/config.yml', dataframe, userdatadir, featureset_name, 'description', 'details', modulename, json.dumps(module_parameters), json.dumps(parent_datasets))
