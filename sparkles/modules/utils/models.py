import uuid

from sqlalchemy import create_engine, Table, Column, String, Text, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

Base = declarative_base()


def config_to_db_session(config_dbpath, Base):
    engine = create_engine(config_dbpath)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


class Analysis(Base):
    __tablename__ = 'analysis'

    id = Column(String(32), primary_key=True)
    name = Column(String(100))
    description = Column(String(250))
    details = Column(Text())
    filepath = Column(String(500), nullable=False)
    created = Column(DateTime(), nullable=False)
    user = Column(String(32), nullable=False)
    parameters = Column(Text())
    inputs = Column(Text())
    outputs = Column(Text())

    def __init__(self):
        pass

    def __init__(self, name, filepath, description, details, created, user, parameters, inputs, outputs):
        self.id = uuid.uuid4().hex
        self.name = name
        self.filepath = filepath
        self.description = description
        self.details = details
        self.created = created
        self.user = user
        self.parameters = parameters
        self.inputs = inputs
        self.outputs = outputs


fs_to_ds = Table("fs_to_ds", Base.metadata, Column("left_fs_id", String, ForeignKey("datasets.id"), primary_key=True), Column("right_ds_id", String, ForeignKey("datasets.id"), primary_key=True))


class Dataset(Base):
    __tablename__ = 'datasets'

    id = Column(String(32), primary_key=True)
    name = Column(String(100))
    fileformat = Column(String(32))
    description = Column(String(250))
    details = Column(Text())
    filepath = Column(String(500), nullable=False)
    created = Column(DateTime(), nullable=False)
    user = Column(String(32), nullable=False)
    module_id = Column(String, ForeignKey('analysis.id'))
    module = relationship('Analysis')
    module_parameters = Column(Text())
    schema = Column(Text())

    parents = relationship("Dataset", secondary="fs_to_ds", primaryjoin="Dataset.id==fs_to_ds.c.left_fs_id", secondaryjoin="Dataset.id==fs_to_ds.c.right_ds_id", backref="derived")

    def __init__(self, name, fileformat, description, details, filepath, user, created, module_id, module_parameters, schema):
        self.id = uuid.uuid4().hex
        self.name = name
        self.fileformat = fileformat
        self.description = description
        self.details = details
        self.filepath = filepath
        self.user = user
        self.created = created
        self.module_parameters = module_parameters
        self.schema = schema
        self.module_id = module_id
