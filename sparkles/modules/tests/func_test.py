from mock import Mock
from sqlalchemy import text
from sparkles.modules.utils.models import Base, Dataset, Analysis, config_to_db_session
from sparkles.modules.tests.side_effects import mod_se, ds_se, call_se, list_ds_se, list_mod_se
from swiftclient.service import SwiftService, SwiftUploadObject
import subprocess
import json


class Functions_Test(object):

    def __init__(self):

        self.config = {'DB_LOCATION': '', 'CLUSTER_URL': '', 'MODULE_LOCAL_STORAGE': '', 'SWIFT_AUTH_URL': '', 'SWIFT_USERNAME': '', 'SWIFT_PASSWORD': '', 'SWIFT_TENANT_ID': '', 'SWIFT_PASSWORD': '', 'SWIFT_TENANT_NAME': ''}
        self.subprocessMock = Mock(spec=subprocess)
        self.session = Mock(spec=config_to_db_session('sqlite:////shared_data/sparkles/etc/sqlite.db', Base))

    def list_modules(self, prefix=''):

        sessionQuery = self.session.query(Analysis).filter(Analysis.name.like('%' + prefix + '%')).all().side_effect = list_mod_se
        modules = sessionQuery(prefix)
        res = []
        for amodule in modules:
            res.append(amodule.name)
        return ','.join(res)

    def list_datasets(self, prefix=''):

        sessionQuery = self.session.query(Dataset).filter(Dataset.name.like('%' + prefix + '%')).all().side_effect = list_ds_se
        datasets = sessionQuery(prefix)
        res = []
        for dataset in datasets:
            if(dataset.module is None):
                res.append(dataset.name)
            else:
                res.append('Featureset: ' + dataset.name)
        return ','.join(res)

    def mock_import_analysis(self, name='', description='', details='', filepath='', params='', inputs='', outputs=''):

        config = {'DB_LOCATION': '', 'SWIFT_AUTH_URL': '', 'SWIFT_USERNAME': '', 'SWIFT_PASSWORD': '', 'SWIFT_TENANT_ID': '', 'SWIFT_PASSWORD': '', 'SWIFT_TENANT_NAME': ''}

        filename = filepath
        created = 'mm-dd-yyyy 00:00:00.000'
        user = 'root'

        session = Mock(spec=config_to_db_session('sqlite:////shared_data/sparkles/etc/sqlite.db', Base))
        sessionQuery = session.query(Analysis).from_statement(text("SELECT * FROM analysis where name=:name")).\
            params(name=name).first().side_effect = mod_se

        checkMod = sessionQuery(name=name)
        if(checkMod is None):

            analysisMod = Mock(spec=Analysis(name=name, filepath=filename, description=description, details=details, created=created, user=user, parameters=params, inputs=inputs, outputs=outputs))
            session.add(analysisMod)
            session.commit()

            options = {'os_auth_url': config['SWIFT_AUTH_URL'], 'os_username': config['SWIFT_USERNAME'], 'os_password': config['SWIFT_PASSWORD'], 'os_tenant_id': config['SWIFT_TENANT_ID'], 'os_tenant_name': config['SWIFT_TENANT_NAME']}
            swiftService = Mock(spec=SwiftService(options=options))
            objects = []
            objects.append(SwiftUploadObject(config['DB_LOCATION'], object_name='sqlite.db'))
            objects.append(SwiftUploadObject(filepath, object_name=filename))

            swiftUpload = swiftService.upload(container='containerModules', objects=objects).return_value = ('Metadata', 'Module')
            for uploaded in swiftUpload:
                print(uploaded)

            return "import_success"

        else:
            raise RuntimeError("Analysis " + name + " already exists")

    def mock_run_analysis(self, modulename='', params=None, inputs=None, features=None):

        config = {'DB_LOCATION': '', 'CLUSTER_URL': '', 'MODULE_LOCAL_STORAGE': '', 'SWIFT_AUTH_URL': '', 'SWIFT_USERNAME': '', 'SWIFT_PASSWORD': '', 'SWIFT_TENANT_ID': '', 'SWIFT_PASSWORD': '', 'SWIFT_TENANT_NAME': ''}
        session = Mock(spec=config_to_db_session('sqlite:////shared_data/sparkles/etc/sqlite.db', Base))

        if(modulename is None or params is None or inputs is None):
            raise RuntimeError("Modulename, params and inputs are necessary")
        else:
            sessionQuery = session.query(Analysis).from_statement(text("SELECT * FROM analysis where name=:name")).\
                params(name=modulename).first().side_effect = mod_se
            analysisMod = sessionQuery(name=modulename)

            if(analysisMod):
                filepaths = ''
                filepathsarr = []

                # Download the module from swift first
                options = {'os_auth_url': config['SWIFT_AUTH_URL'], 'os_username': config['SWIFT_USERNAME'], 'os_password': config['SWIFT_PASSWORD'], 'os_tenant_id': config['SWIFT_TENANT_ID'], 'os_tenant_name': config['SWIFT_TENANT_NAME']}

                swiftService = Mock(spec=SwiftService(options=options))

                out_file = config['MODULE_LOCAL_STORAGE'] + analysisMod.filepath
                localoptions = {'out_file': out_file}
                objects = []
                objects.append(analysisMod.filepath)
                swiftDownload = swiftService.download(container='containerModules', objects=objects, options=localoptions).return_value = ('Module download', '')

                for downloaded in swiftDownload:
                    print(downloaded)

                for inputfile in inputs:
                    sessionDsQuery = session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
                        params(name=inputfile).first().side_effect = ds_se

                    dataset = sessionDsQuery(name=inputfile)
                    if(dataset):
                        filepathsarr.append(dataset.filepath)

                    if(len(filepathsarr) <= 0):
                        raise RuntimeError("The specified datasets could not be found")

                filepaths = json.dumps(filepathsarr)
                params = json.dumps(params)

                helperpath = 'file:///helperpath'
                subprocessMock = Mock(spec=subprocess)
                subprocessMock.call.side_effect = call_se
                if(features is None):
                    s = subprocessMock.call(["/opt/spark/bin/pyspark", out_file, "--master", config['CLUSTER_URL'], helperpath, params, filepaths])
                else:
                    features['configpath'] = 'configpath'
                    features = json.dumps(features)
                    s = subprocessMock.call(["/opt/spark/bin/pyspark", out_file, "--master", config['CLUSTER_URL'], helperpath, params, filepaths, features])
                return s

            else:
                raise RuntimeError("Analysis module not found")

    def mock_import_dataset(self, inputfiles=[], description='', details='', userdatadir=''):

        if(inputfiles and len(inputfiles) > 0):

            if(not userdatadir and userdatadir == ''):
                raise RuntimeError("User data directory is required")

            # path = dirname(dirname(os.path.abspath(__file__)))
            path = '/relative/path'
            configpath = ''
            originalpaths = json.dumps(inputfiles)
            self.subprocessMock.call(["/opt/spark/bin/pyspark", path + "/data_import.py", "--master", self.config['CLUSTER_URL'], originalpaths, description, details, userdatadir, configpath])
            return 'import_success'
        else:
            raise RuntimeError("Please ensure inputs is not None or empty")

# f = Functions_Test()
# print(f.list_modules('Ev'))
# print(f.list_datasets())
# inputfiles = ['/path/to/dataset.h5', '/path/to/anotherdataset.h5']
# f.mock_import_dataset(inputfiles=inputfiles, userdatadir='')
# f.test_run_analysis(modulename='existing', inputs=inputs, params={})
# f.test_run_analysis(modulename='existing', inputs=inputs, params={}, features={})

# f.mock_import_analysis(name='existing_1module', filepath='etc')
