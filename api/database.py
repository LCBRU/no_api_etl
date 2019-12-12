"""Database context manager
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from sqlalchemy.ext.declarative import declarative_base
from api.environment import ETL_CENTRAL_CONNECTION_STRING, ETL_DATABASES_CONNECTION_STRING, DATABASE_ECHO

Base = declarative_base()


@contextmanager
def etl_central_session():
    try:
        engine = create_engine(ETL_CENTRAL_CONNECTION_STRING, echo=DATABASE_ECHO)
        session_maker = sessionmaker(bind=engine)
        session = session_maker()
        yield session

    except Exception as e:
        session.rollback()
        session.close()
        raise e
    else:
        session.commit()
        session.close()
    finally:
        engine.dispose


@contextmanager
def etl_databases_engine():
    engine = create_engine(ETL_DATABASES_CONNECTION_STRING, echo=DATABASE_ECHO)
    yield engine
    engine.dispose()


@contextmanager
def engine(connection_string):
    engine = create_engine(connection_string, echo=DATABASE_ECHO)
    yield engine
    engine.dispose()

@contextmanager
def connection(connection_string):
    engine = create_engine(connection_string, echo=DATABASE_ECHO)
    connection = engine.connect()
    try:
        yield engine
    finally:
        connection.close()
