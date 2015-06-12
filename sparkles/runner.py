from sqlalchemy import text
from sparkles.models import Base, config_to_db_session, fs_to_ds, Dataset, Analysis
from datetime import datetime
import getpass
import re


class SparkRunner(object):

    def __init__(self, config):
        print(config)
        self.session = config_to_db_session(config, Base)

    def list_analysises(self):
        raise NotImplementedError

    def list_datasets(self):
        print('List of available datasets')
        datasets = self.session.query(Dataset).all()
        for dataset in datasets:
            print(dataset.name)

    def run_analysis(self, *args, **kwargs):
        raise NotImplementedError

    def create_dataset(self)
        pass

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
        module_id = am.id

        ds = Dataset(name=fileid, description=params['description'], details=params['details'], module_parameters=params['module_parameters'], created=params['created'], user=params['user'], fileformat="Parquet", filepath=params['filepath'], schema=params['schema'], module_id=am.id)
        self.session.add(ds)
        self.session.commit()


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

        am = self.session.query(Analysis).first()
        print(am.name)
        print(am.created)
        print(am.inputs)

    def test_insert(self, fileinp):
        fileinp = fileinp.split('.')
        fileid = fileinp[0]
        filepath = "/shared_data/files/FI4000047485/"
        created = datetime.now()
        user = getpass.getuser()
        am = self.session.query(Analysis).first()
        ds = Dataset(name=fileid, description="", details="", module_parameters="", created=created, user=user, fileformat="Parquet", filepath=filepath, schema="", module_id=am.id)
        self.session.add(ds)
        self.session.commit()

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
