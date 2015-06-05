import uuid

from sqlalchemy import create_engine, Column, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


def config_to_db_session(config, Base):
    engine = create_engine(config['DATABASE_URI'])
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


class Dataset(Base):
    __tablename__ = 'datasets'

    id = Column(String(32), primary_key=True)

    def __init__(self):
        self.id = uuid.uuid4().hex


class Analysis(Base):
    __tablename__ = 'analysis'

    id = Column(String(32), primary_key=True)

    def __init__(self):
        self.id = uuid.uuid4().hex
