from sqlalchemy import text
from models import Base, config_to_db_session, Dataset, Analysis
from datetime import datetime
import getpass
from helper import saveObjsBackend, getObjsBackend
from subprocess import call
import yaml
import os
from os.path import dirname
from swiftclient.service import SwiftService, SwiftUploadObject
import json
import warnings
import shutil
import socket
from hdfs import InsecureClient
import hdfs


class SparkRunner(object):

    def __init__(self, configpath=None):

        config = None
        with open(configpath, 'r') as config_file:
            config = yaml.load(config_file)

        self.backend = config['BACKEND']  # Choose the backend from hdfs or swift
        out_file = config['DB_LOCATION']  # Where to store the metadata on local system
        objs = []
        if(self.backend == 'hdfs'):
            objs.append(('/modules/sqlite.db', out_file))
        elif(self.backend == 'swift'):
            objs.append('sqlite.db', out_file)

        getObjsBackend(objs, self.backend)

        dburi = config['DATABASE_URI']
        self.clusterUrl = "spark://" + socket.gethostname() + ':' + config['CLUSTER_PORT']
        self.session = config_to_db_session(dburi, Base)
        self.config = config
        self.configpath = configpath
        self.hdfsmodpath = '/modules/'

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
            shutil.copyfile(self.config['DB_LOCATION'], '/shared_data/sparkles/tmp/sqlite_temp.db')  # Backup metadata

            self.session.add(analysisMod)
            self.session.commit()
            # Upload the metadata and module to swift
            objs = []
            if(self.backend == 'hdfs'):
                objs.append((self.hdfsmodpath, self.config['DB_LOCATION']))
                objs.append((self.hdfsmodpath, filepath))
            elif(self.backend == 'swift'):
                objs.append(('sqlite.db', self.config['DB_LOCATION']))
                objs.append((filename, filepath))

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

                # Download the module from swift first
                out_file = self.config['MODULE_LOCAL_STORAGE'] + analysisMod.filepath

                objs = []
                objs.append((self.hdfsmodpath + analysisMod.filepath, out_file))
                getObjsBackend(objs, self.backend)  # Act acciording to the backend choice

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

                if(features is None):
                    call(["/opt/spark/bin/pyspark", out_file, "--master", self.clusterUrl, self.backend, helperpath, params, filepaths])
                else:  # When there's a featureset to be saved from the module
                    if('userdatadir' not in features):
                        if(self.backend == 'hdfs'):
                            features['userdatadir'] = 'hdfs://' + socket.gethostname() + ':9000' + '/features'
                        elif(self.backend == 'swift'):
                            features['userdatadir'] = 'swift://containerFeatures.SparkTest'

                    features['configpath'] = self.configpath  # The configpath is passed as a default parameter
                    features = json.dumps(features)
                    call(["/opt/spark/bin/pyspark", out_file, "--master", self.clusterUrl, self.backend, helperpath, params, filepaths, features])

            else:
                raise RuntimeError("Analysis module not found")

    def import_dataset(self, inputfiles=[], description='', details='', userdatadir=''):

        ''' Imports a given dataset (on a local path) to the backend which is a Swift object store
        Multiple files can be imported as inputfiles parameters is an array. The userdatadir is the object store container URI
        '''

        if(inputfiles):

            if(not userdatadir):
                if(self.backend == 'hdfs'):
                    userdatadir = 'hdfs://' + socket.gethostname() + ':9000' + '/files'
                elif(self.backend == 'swift'):
                    userdatadir = 'swift://containerFiles.SparkTest'

            path = dirname(dirname(os.path.abspath(__file__)))
            configpath = self.configpath
            originalpaths = json.dumps(inputfiles)
            call(["/opt/spark/bin/pyspark", path + "/data_import.py", "--master", self.clusterUrl, self.backend, originalpaths, description, details, userdatadir, configpath])

        else:
            raise RuntimeError("Please ensure inputfiles is not None or empty")
