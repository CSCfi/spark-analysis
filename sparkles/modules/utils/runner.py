from sqlalchemy import text
from models import Base, config_to_db_session, Dataset, Analysis
from datetime import datetime
import getpass
from helper import saveObjsBackend, getObjsBackend, delete_item
from subprocess import call
import yaml
import os
from os.path import dirname
from swiftclient.service import SwiftService, SwiftUploadObject
import json
import warnings
import shutil
import socket
import re
from urlparse import urlparse


class SparkRunner(object):

    def __init__(self, configpath=None):

        config = None
        if not configpath:
            configpath = '/sparkles/etc/config.yml'

        with open(configpath, 'r') as config_file:
            config = yaml.load(config_file)

        self.backend = config['BACKEND']  # Choose the backend from hdfs or swift
        out_file = config['METADATA_LOCAL_PATH']  # Where to store the metadata on local system
        objs = []
        if(self.backend == 'hdfs'):
            objs.append((config['MODULES_DIR'] + 'sqlite.db', out_file))
        elif(self.backend == 'swift'):
            objs.append('sqlite.db', out_file)
        elif(self.backend == 'nfs'):
            pass  # Metadata already in right local dir

        getObjsBackend(objs, self.backend, config)

        dburi = config['METADATA_URI']
        self.clusterUrl = config['CLUSTER_URL']
        self.session = config_to_db_session(dburi, Base)
        self.config = config
        self.configstr = json.dumps(config)
        self.hdfsmodpath = config['MODULES_DIR']
        self.backup_metadata_path = config['BACKUP_METADATA_LOCAL_PATH']
        self.hadoop_port = config['HADOOP_RPC_PORT']

    def list_modules(self, prefix=''):

        ''' Searches for the modules according to the given prefix.
        If no prefix is given, it returns all the modules in the backend
        '''

        print('List of available modules:')
        print('--------------------------')

        analysisModules = self.session.query(Analysis).filter(Analysis.name.like('%' + prefix + '%')).all()
        for amodule in analysisModules:
            print('Name: ' + amodule.name + '|Description: ' + amodule.description + '|Details: ' + amodule.details)

    def list_datasets(self, prefix=''):

        ''' Searches for the datasets/featuresets according to the given prefix.
        If no prefix is given, it returns all the datasets/featuresets in the backend
        '''

        print('List of available datasets:')
        print('---------------------------')
        datasets = self.session.query(Dataset).filter(Dataset.name.like('%' + prefix + '%')).all()
        for dataset in datasets:
            if(dataset.module is None):
                print(dataset.name + '|Description: ' + dataset.description + '|Details: ' + dataset.details)
            else:
                print(dataset.name + '|Description: ' + dataset.description + '|Details: ' + dataset.details + '|Module used: ' + dataset.module.name + '|Parameters used: ' + dataset.module_parameters + '|Parents: ' + json.dumps(list(map((lambda x: x.name), dataset.parents))))
            print('****************************')

    def import_analysis(self, name='', description='', details='', filepath='', params='', inputs='', outputs=''):

        ''' Imports the analysis module (present on the local path) written in Spark RDD language in the backend.
        The backend is a Swift Object Store. Parameters are needed to update the metadata.
        '''

        filename = os.path.basename(filepath)
        created = datetime.now()
        user = getpass.getuser()

        checkMod = self.session.query(Analysis).from_statement(text("SELECT * FROM analysis where name=:name")).\
            params(name=name).first()

        if(checkMod is None):
            analysisMod = Analysis(name=name, filepath=filename, description=description, details=details, created=created, user=user, parameters=params, inputs=inputs, outputs=outputs)
            shutil.copyfile(self.config['METADATA_LOCAL_PATH'], self.backup_metadata_path)  # Backup metadata

            self.session.add(analysisMod)
            self.session.commit()
            # Upload the metadata and module to swift
            objs = []
            if(self.backend == 'hdfs'):
                objs.append((self.hdfsmodpath, self.config['METADATA_LOCAL_PATH']))
                objs.append((self.hdfsmodpath, filepath))
            elif(self.backend == 'swift'):
                objs.append(('sqlite.db', self.config['METADATA_LOCAL_PATH']))
                objs.append((filename, filepath))
            elif(self.backend == 'nfs'):
                objs.append((filename, filepath))  # Send only the module

            saveObjsBackend(objs, self.backend, self.config)
        else:
            raise RuntimeError("Analysis " + name + " already exists")

    def run_analysis(self, modulename='', params=None, inputs=None, features=None):

        ''' Runs the given analysis module against the given input datasets and produces the output.
        The analysis module and the datasets have to be imported first (Metadata is read)
        The output can be printed on console or saved as a featureset (features parameter needs to be specified)
        '''

        if(modulename is None or params is None or inputs is None):
            raise RuntimeError("Modulename, params and inputs are necessary")
        else:
            analysisMod = self.session.query(Analysis).from_statement(text("SELECT * FROM analysis where name=:name")).\
                params(name=modulename).first()
            if(analysisMod):
                filepaths = ''
                filepathsarr = []

                # Download the module from Storage first
                out_file = self.config['MODULES_DIR'] + analysisMod.filepath

                objs = []

                if(self.config['BACKEND'] == 'hdfs'):
                    objs.append((self.hdfsmodpath + analysisMod.filepath, out_file))
                elif(self.config['BACKEND'] == 'swift'):
                    objs.append((analysisMod.filepath, out_file))
                elif(self.config['BACKEND'] == 'nfs'):
                    pass

                getObjsBackend(objs, self.backend, self.config)  # Act acciording to the backend choice

                for inputfile in inputs:
                    dataset = self.session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
                        params(name=inputfile).first()
                    if(dataset):
                        filepathsarr.append(dataset.filepath)

                if(not filepathsarr):
                    raise RuntimeError("No datasets found")

                filepaths = json.dumps(filepathsarr)
                params = json.dumps(params)

                helperpath = dirname(dirname(os.path.abspath(__file__)))

                shuffle_partitions = str(self.config['SHUFFLE_PARTITIONS'])

                if(features is None):
                    call([self.config['PYSPARK_CLIENT_PATH'], out_file, "--master", self.clusterUrl, self.backend, helperpath, params, filepaths])
                else:  # When there's a featureset to be saved from the module
                    if('userdatadir' not in features):
                        if(self.backend == 'hdfs'):
                            features['userdatadir'] = 'hdfs://' + socket.gethostname() + ':' + str(self.hadoop_port) + self.config['FEATURES_DIR']
                        elif(self.backend == 'swift'):
                            features['userdatadir'] = 'swift://containerFeatures.SparkTest'
                        elif(self.backend == 'nfs'):
                            features['userdatadir'] = 'file://' + self.config['FEATURES_DIR']

                    features['configstr'] = self.configstr  # The configstr is passed as a default parameter
                    features['modulename'] = modulename
                    features = json.dumps(features)
                    call([self.config['PYSPARK_CLIENT_PATH'], out_file, "--master", self.clusterUrl, self.backend, helperpath, shuffle_partitions, params, filepaths, features])

            else:
                raise RuntimeError("Analysis module not found")

    def test_analysis(self, modulepath='', params=None, inputs=None, features=None):

        ''' Tests the given analysis module against the given input datasets and produces the output.
            It is recommended to run this on local mode with small sized input
        '''

        if(modulepath is None or params is None or inputs is None):
            raise RuntimeError("modulepath, params and inputs are necessary")
        else:
                filepaths = ''
                filepathsarr = []

                for inputfile in inputs:
                    dataset = self.session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
                        params(name=inputfile).first()
                    if(dataset):
                        filepathsarr.append(dataset.filepath)

                if(not filepathsarr):
                    raise RuntimeError("No datasets found")

                filepaths = json.dumps(filepathsarr)
                params = json.dumps(params)

                helperpath = dirname(dirname(os.path.abspath(__file__)))

                shuffle_partitions = str(self.config['SHUFFLE_PARTITIONS'])

                if not features:
                    features = {}
                features['module_testing'] = True  # Introduce a parameter to keep the system informed that we are testing the module
                features = json.dumps(features)
                call([self.config['PYSPARK_CLIENT_PATH'], modulepath, "--master", "local[*]", self.backend, helperpath, shuffle_partitions, params, filepaths, features])

    def import_dataset(self, inputfiles=[], description='', details='', userdatadir=''):

        ''' Imports a given dataset (on a local path) to the backend which is a Swift object store
        Multiple files can be imported as inputfiles parameters is an array. The userdatadir is the object store container URI
        '''

        if(inputfiles):

            if(not userdatadir):
                if(self.backend == 'hdfs'):
                    userdatadir = 'hdfs://' + socket.gethostname() + ':' + str(self.hadoop_port) + self.config['FILES_DIR']
                elif(self.backend == 'swift'):
                    userdatadir = 'swift://containerFiles.SparkTest'
                elif(self.backend == 'nfs'):
                    userdatadir = 'file://' + self.config['FILES_DIR']

            path = dirname(dirname(os.path.abspath(__file__)))
            configstr = self.configstr
            originalpaths = json.dumps(inputfiles)
            partitions = str(self.config['IMPORT_PARTITIONS'])

            call([self.config['PYSPARK_CLIENT_PATH'], path + "/data_import.py", "--master", self.clusterUrl, self.backend, originalpaths, description, details, userdatadir, configstr, partitions])

        else:
            raise RuntimeError("Please ensure inputfiles is not None or empty")

    def drop_analysis(self, modulename=''):

        if(modulename is None):
            raise RuntimeError("Module name is required")
        else:

            analysisMod = self.session.query(Analysis).filter_by(name=modulename).first()

            if analysisMod:
                localpath = self.config['MODULES_DIR'] + analysisMod.filepath
                if(self.config['BACKEND'] == 'hdfs'):
                    delete_item(self.config, filepath=self.config['MODULES_DIR'] + analysisMod.filepath, localpath=localpath)
                elif(self.config['BACKEND'] == 'swift'):
                    pass  # To be implemented
                elif(self.config['BACKEND'] == 'nfs'):
                    delete_item(self.config, localpath=localpath)  # Only local dirs required in nfs

                self.session.delete(analysisMod)
                self.session.commit()
                print('Module deleted')
            else:
                raise RuntimeError('Module does not exist')

    def drop_dataset(self, datasetname=''):

        if(datasetname is None):
            raise RuntimeError("Dataset name is required")
        else:

            dataset = self.session.query(Dataset).filter_by(name=datasetname).first()
            if dataset:
                if(self.config['BACKEND'] == 'hdfs'):
                    u = urlparse(dataset.filepath)
                    delete_item(self.config, filepath=u.path)
                elif(self.config['BACKEND'] == 'swift'):
                    pass  # To be implemented
                elif(self.config['BACKEND'] == 'nfs'):
                    u = urlparse(dataset.filepath)
                    delete_item(self.config, localpath=u.path)

                self.session.delete(dataset)
                self.session.commit()
                print('Dataset deleted')
            else:
                raise RuntimeError('Dataset does not exist')
