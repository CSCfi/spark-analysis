import uuid

from sqlalchemy import create_engine, Column, String, Text, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

Base = declarative_base()


def config_to_db_session(config, Base):
    engine = create_engine(config['DATABASE_URI'])
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

    def __init__(self, name, filepath, created, user, inputs):
        self.id = uuid.uuid4().hex
        self.name = name
        self.filepath = filepath
        self.created = created
        self.user = user
        self.inputs = inputs


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
    parents = Column(Text())
    parameters = Column(Text())
    fields = Column(Text())

    def __init__(self, name, fileformat, filepath, user, created, module_id, fields):
        self.id = uuid.uuid4().hex
        self.name = name
        self.fileformat = fileformat
        self.filepath = filepath
        self.user = user
        self.created = created
        self.fields = fields
        self.module_id = module_id
