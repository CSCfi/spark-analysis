from sqlalchemy import text
from models import Base, config_to_db_session, fs_to_ds, Dataset, Analysis
from datetime import datetime
import getpass
import re
from subprocess import call
import yaml
import os
from os.path import dirname
import shutil
import errno
from swiftclient.service import *
import json


class SparkRunner(object):

    def __init__(self, configpath=None):

        config = None
        if(configpath is None):
            configpath = '/shared_data/etc/config.yml'
        with open(configpath, 'r') as config_file:
            config = yaml.load(config_file)

        print(config)
        self.session = config_to_db_session(config, Base)
        self.config = config

    def list_modules(self):

        print('List of available modules')
        analysismodules = self.session.query(Analysis).all()
        for amodule in analysismodules:
            print('Name: ' + amodule.name + '|Description: ' + amodule.description + '|Details: ' + amodule.details)

    def list_datasets(self):
        print('List of available datasets')
        datasets = self.session.query(Dataset).all()
        for dataset in datasets:
            if(dataset.module is None):
                print(dataset.name + '|Description: ' + dataset.description + '|Details: ' + dataset.details)
            else:
                print(dataset.name + '|Description: ' + dataset.description + '|Details: ' + dataset.details + '|Module used: ' + dataset.module.name + '|Parameters used: ' + dataset.module_parameters + '|Parents: ' + json.dumps(list(map((lambda x: x.name), dataset.parents))))
            print('****************************')

    def import_analysis(self, destination, name, description, details, filepath, params, inputs, outputs):

        src = filepath
        dst = destination
        if(src.endswith('/')):
            src = src[:-1]
        if(not dst.endswith('/')):
            dst = dst + '/'

        filename = os.path.basename(src)
        dst = dst + filename
        # shutil.copy(src, dst)
        created = datetime.now()
        user = getpass.getuser()

        checkMod = self.session.query(Analysis).from_statement(text("SELECT * FROM analysis where name=:name")).\
            params(name=name).first()

        if(checkMod is None):
            analysisMod = Analysis(name=name, filepath=filename, description=description, details=details, created=created, user=user, parameters=params, inputs=inputs, outputs=outputs)
            self.session.add(analysisMod)
            self.session.commit()

            # Upload the metadata and module to swift
            options = {'os_auth_url': self.config['SWIFT_AUTH_URL'], 'os_username': self.config['SWIFT_USERNAME'], 'os_password': self.config['SWIFT_PASSWORD'], 'os_tenant_id': self.config['SWIFT_TENANT_ID'], 'os_tenant_name': self.config['SWIFT_TENANT_NAME']}
            swiftService = SwiftService(options=options)
            objects = []
            objects.append(SwiftUploadObject(self.config['DB_LOCATION'], object_name='sqlite.db'))
            objects.append(SwiftUploadObject(filepath, object_name=filename))

            swiftUpload = swiftService.upload(container='containerModules', objects=objects)
            uploadedIndex = 0
            for uploaded in swiftUpload:
                if(uploadedIndex == 1):
                    print('Metadata changed and uploaded')
                elif(uploadedIndex == 2):
                    print('Module uploaded')
                uploadedIndex = uploadedIndex + 1
        else:
            print("Analysis " + name + " already exists")

    def run_analysis(self, modulename=None, params=None, inputs=None, features=None):

        if(modulename is None or params is None or inputs is None):
            print('Modulename, params and inputs are mandatory')
        else:
            analysisMod = self.session.query(Analysis).from_statement(text("SELECT * FROM analysis where name=:name")).\
                params(name=modulename).first()
            filepaths = ''
            filepathsarr = []

            # Download the module from swift first
            options = {'os_auth_url': self.config['SWIFT_AUTH_URL'], 'os_username': self.config['SWIFT_USERNAME'], 'os_password': self.config['SWIFT_PASSWORD'], 'os_tenant_id': self.config['SWIFT_TENANT_ID'], 'os_tenant_name': self.config['SWIFT_TENANT_NAME']}
            swiftService = SwiftService(options=options)

            out_file = '/shared_data/mods/' + analysisMod.filepath
            localoptions = {'out_file': out_file}
            objects = []
            objects.append(analysisMod.filepath)
            swiftDownload = swiftService.download(container='containerModules', objects=objects, options=localoptions)

            for downloaded in swiftDownload:
                print(downloaded)

            for inputfile in inputs:
                dataset = self.session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
                    params(name=inputfile).first()
                filepathsarr.append(dataset.filepath)

            filepaths = json.dumps(filepathsarr)
            params = json.dumps(params)
            features = json.dumps(features)

            if(features is None):
                call(["/opt/spark/bin/pyspark", out_file, "--master", self.config['CLUSTER_URL'], params, filepaths])
            else:
                call(["/opt/spark/bin/pyspark", out_file, "--master", self.config['CLUSTER_URL'], params, filepaths, features])

    def import_dataset(self, inputs, description, details, userdatadir):

        # am = self.session.query(Analysis).from_statement(text("SELECT * FROM analysis where name=:name")).\
        #    params(name="dataimport").first()
        path = dirname(dirname(os.path.abspath(__file__)))
        call(["/opt/spark/bin/pyspark", path + "/data_import.py", "--master", self.config['CLUSTER_URL'], inputs, description, details, userdatadir])

    def create_dataset(self, params):

        checkDataset = self.session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
            params(name=params['name']).first()

        if(checkDataset is None):

            dataset = Dataset(name=params['name'], description=params['description'], details=params['details'], module_parameters='', created=params['created'], user=params['user'], fileformat="Parquet", filepath=params['filepath'], schema=params['schema'], module_id='')
            self.session.add(dataset)
            self.session.commit()

            options = {'os_auth_url': self.config['SWIFT_AUTH_URL'], 'os_username': self.config['SWIFT_USERNAME'], 'os_password': self.config['SWIFT_PASSWORD'], 'os_tenant_id': self.config['SWIFT_TENANT_ID'], 'os_tenant_name': self.config['SWIFT_TENANT_NAME']}
            swiftService = SwiftService(options=options)
            objects = []
            objects.append(SwiftUploadObject(self.config['DB_LOCATION'], object_name='sqlite.db'))

            swiftUpload = swiftService.upload(container='containerModules', objects=objects)
            for uploaded in swiftUpload:
                print("Metadata changed and uploaded")

        else:
            raise ValueError("The dataset with name " + params['name'] + " already exists")

    def create_relation(self, featset, parents):

        featureset = self.session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
            params(name=featset).first()
        print(featset)
        print(featureset)
        parents = json.loads(parents)
        print(parents)
        for parent in parents:
            dataset = self.session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
                params(name=parent).first()
            print(dataset)
            f = fs_to_ds.insert().values(left_fs_id=featureset.id, right_ds_id=dataset.id)
            self.session.execute(f)

        self.session.commit()

    def create_featureset(self, params):

        modulename = params['modulename']
        analysisMod = self.session.query(Analysis).from_statement(text("SELECT * FROM analysis where name=:name")).\
            params(name=modulename).first()

        if(analysisMod is not None):  # Check if the module exists

            module_id = analysisMod.id
            checkDataset = self.session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
                params(name=params['name']).first()

            print('Features ' + params['name'])
            if(checkDataset is None):
                dataset = Dataset(name=params['name'], description=params['description'], details=params['details'], module_parameters=params['module_parameters'], created=params['created'], user=params['user'], fileformat="Parquet", filepath=params['filepath'], schema=params['schema'], module_id=analysisMod.id)
                self.session.add(dataset)
                self.session.commit()

                options = {'os_auth_url': self.config['SWIFT_AUTH_URL'], 'os_username': self.config['SWIFT_USERNAME'], 'os_password': self.config['SWIFT_PASSWORD'], 'os_tenant_id': self.config['SWIFT_TENANT_ID'], 'os_tenant_name': self.config['SWIFT_TENANT_NAME']}
                swiftService = SwiftService(options=options)
                objects = []
                objects.append(SwiftUploadObject(self.config['DB_LOCATION'], object_name='metadata'))
                swiftUpload = swiftService.upload(container='containerModules', objects=objects)
                for uploaded in swiftUpload:
                    print("Metadata changed and uploaded")
            else:
                raise ValueError('The feature set with the name ' + params['name'] + ' already exists')
        else:
            raise ValueError('No Such Module')
