from sqlalchemy import text
from models import Base, config_to_db_session, fs_to_ds, Dataset, Analysis
from datetime import datetime
import getpass
import re
from subprocess import call


class SparkRunner(object):

    def __init__(self, config):
        print(config)
        self.session = config_to_db_session(config, Base)

    def list_analysises(self):

        print('List of available modules')
        analysismodules = self.session.query(Analysis).all()
        for am in analysismodules:
            print(am.name)

    def list_datasets(self):
        print('List of available datasets')
        datasets = self.session.query(Dataset).all()
        for dataset in datasets:
            print(dataset.name)

    def import_analysis(self, name, description, details, filepath, params, inputs, outputs):

        src = filepath
        dst = "modules/"
        if(src.endswith('/')):
            src = src[:-1]
        if(not dst.endswith('/')):
            dst = dst + '/'

        filename = os.path.basename(src)
        dst = dst + filename
        try:
            shutil.copytree(src, dst)
        except OSError as exc:  # If the module is not a package
            if exc.errno == errno.ENOTDIR:
                shutil.copy(src, dst)
            else:
                raise NotImplementedError

        created = datetime.now()
        user = getpass.getuser()

        am = Analysis(name=name, filepath=filepath, description=description, details=details, created=created, user=user, parameters=params, inputs=inputs, outputs=outside)

        self.session.add(am)
        self.session.commit()

    def run_analysis(self, modulename, params, inputs):

        am = self.session.query(Analysis).from_statement(text("SELECT * FROM analysis where name=:name")).\
            params(name=modulename).first()

        inputs = inputs.split(',')
        filepaths = ''
        for inputfile in inputs:
            ds = self.session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
                params(name=inputfile).first()
            filepath = filepath + ',' + ds.filepath

        call(["/opt/spark/bin/pyspark", "", am.filepath, "--master", "spark://nandan-spark-cluster-fe:7077", params, filepaths])

    def import_dataset(self, inputs, description, details):

        # am = self.session.query(Analysis).from_statement(text("SELECT * FROM analysis where name=:name")).\
        #    params(name="dataimport").first()
        call(["/opt/spark/bin/pyspark", "/shared_data/github/spark-analysis/modules/data_import.py", "--master", "spark://nandan-spark-cluster-fe:7077", inputs, description, details])

    def create_dataset(self, params):

        d = self.session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
            params(name=params['name']).first()

        if(d is None):

            ds = Dataset(name=params['name'], description=params['description'], details=params['details'], module_parameters='', created=params['created'], user=params['user'], fileformat="Parquet", filepath=params['filepath'], schema=params['schema'], module_id='')
            self.session.add(ds)
            self.session.commit()
        else:
            raise ValueError("The dataset with name " + params['name'] + " already exists")

    def create_relation(self, featset, parents):

        fs = self.session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
            params(name=featset).first()
        parents = parents.split(',')
        for p in parents:
            dss = self.session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
                params(name=p).first()

            f = fs_to_ds.insert().values(left_fs_id=fs.id, right_ds_id=dss.id)
            self.session.execute(f)

        self.session.commit()

    def create_featureset(self, params):

        modulename = params['modulename']
        am = self.session.query(Analysis).from_statement(text("SELECT * FROM analysis where name=:name")).\
            params(name=modulename).first()

        if(am is not None):  # Check if the module exists

            module_id = am.id
            d = self.session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
                params(name=params['name']).first()

            if(d is None):
                ds = Dataset(name=params['name'], description=params['description'], details=params['details'], module_parameters=params['module_parameters'], created=params['created'], user=params['user'], fileformat="Parquet", filepath=params['filepath'], schema=params['schema'], module_id=am.id)
                self.session.add(ds)
                self.session.commit()
            else:
                raise ValueError('The feature set with the name ' + params['name'] + ' already exists')
        else:
            raise ValueError('No Such Module')

    def test_analysis(self):
        name = __file__
        p = re.compile('.+/(\w+)\.\w+')
        m = p.match(name)
        name = m.group(1)
        filepath = '/shared_data/modules/test.py'
        created = datetime.now()
        user = getpass.getuser()

        am = Analysis(name=name, filepath=filepath, description="Counts the events", details="", created=created, user=user, parameters="Time interval", inputs="Market table", outputs="Feature dataset with time and counts")
        self.session.add(am)
        self.session.commit()

    def query_analysis(self, module_id):

        am = self.session.query(Analysis).from_statement(text("SELECT * FROM analysis where name=:name")).\
            params(name=module_id).first()

        # am = self.session.query(Analysis).first()
        if(am is not None):
            print(am.name)
            print(am.created)
            print(am.inputs)
        else:
            raise NameError('No Such Module')

    def test_insert(self, fileinp):

        fileinp = fileinp.split('.')
        fileid = fileinp[0]

        d = self.session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
            params(name=fileid).first()
        if(d is None):
            filepath = "/shared_data/files/FI4000047485/"
            created = datetime.now()
            user = getpass.getuser()
            am = self.session.query(Analysis).first()
            ds = Dataset(name=fileid, description="", details="", module_parameters="", created=created, user=user, fileformat="Parquet", filepath=filepath, schema="", module_id=am.id)
            self.session.add(ds)
            self.session.commit()
        else:
            raise ValueError("The dataset with the name " + fileid + " already exists")

    def test_query(self, fileid):
        dss = self.session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
            params(name=fileid).all()
        for ds in dss:
            print(ds.name)
            print(ds.parents)
            print(ds.derived)
            print(ds.id)
            print(ds.module_id)
            print(ds.module.name)
            print(ds.module.filepath)
