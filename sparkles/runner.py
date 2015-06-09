from sqlalchemy import text
from sparkles.models import Base, config_to_db_session, Dataset, Analysis
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
        raise NotImplementedError

    def run_analysis(self, *args, **kwargs):
        raise NotImplementedError

    def test_insert(self, fileinp):
        fileinp = fileinp.split('.')
        fileid = fileinp[0]
        filesuffix = fileinp[1]
        filepath = "/shared_data/files/FI4000047485_EUR.h5"
        created = datetime.now()
        user = getpass.getuser()
        fileformat = 'Parquet' if(filesuffix == 'parquet') else 'HDF5'
        fields = "ref:string, obid:int32"
        am = self.session.query(Analysis).first()
        ds = Dataset(name=fileid, created=created, user=user, fileformat=fileformat, filepath=filepath, fields=fields, module_id=am.id)

        self.session.add(ds)
        self.session.commit()

    def test_query(self, fileid):
        # ds = self.session.query(Dataset).first()

        dss = self.session.query(Dataset).from_statement(text("SELECT * FROM datasets where name=:name")).\
            params(name=fileid).all()

        for ds in dss:
            print(ds.name)
            print(ds.created)
            print(ds.user)
            print(ds.fields)
            print(ds.fileformat)
            print(ds.id)
            print(ds.module_id)
            print(ds.module.name)

    def test_analysis(self):
        name = __file__
        p = re.compile('.+/(\w+)\.\w+')
        m = p.match(name)
        name = m.group(1)
        filepath = '/shared_data/modules/test.py'
        created = datetime.now()
        user = getpass.getuser()
        inputs = 'FI01.parquet'

        am = Analysis(name=name, filepath=filepath, created=created, user=user, inputs=inputs)
        self.session.add(am)
        self.session.commit()

    def query_analysis(self, module_id):

        am = self.session.query(Analysis).first()
        print(am.name)
        print(am.created)
        print(am.inputs)
