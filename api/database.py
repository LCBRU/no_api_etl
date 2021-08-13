"""Database context manager
"""
import pyodbc
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from sqlalchemy.ext.declarative import declarative_base
from api.environment import (
    ETL_CENTRAL_CONNECTION_STRING,
    DATABASE_ECHO,
    MS_SQL_UHL_DWH_HOST,
    MS_SQL_UHL_DWH_USER,
    MS_SQL_UHL_DWH_PASSWORD,
    MS_SQL_DWH_HOST,
    MS_SQL_DWH_USER,
    MS_SQL_DWH_PASSWORD,
    MS_SQL_ODBC_DRIVER,
)

Base = declarative_base()


@contextmanager
def etl_central_session():
    engine = create_engine(ETL_CENTRAL_CONNECTION_STRING, echo=DATABASE_ECHO)
    session_maker = sessionmaker(bind=engine)
    session = session_maker()

    try:
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
def brc_dwh_cursor(database=None):
    if not database:
        database = 'master'

    conn = pyodbc.connect(
        f'DRIVER={MS_SQL_ODBC_DRIVER};SERVER={MS_SQL_DWH_HOST};DATABASE={database};UID={MS_SQL_DWH_USER};PWD={MS_SQL_DWH_PASSWORD}',
        autocommit=True
    )

    csr = conn.cursor()

    yield csr

    csr.close()
    conn.close()


@contextmanager
def uhl_dwh_databases_engine():
    connectionstring = f'mssql+pyodbc://{MS_SQL_UHL_DWH_USER}:{MS_SQL_UHL_DWH_PASSWORD}@{MS_SQL_UHL_DWH_HOST}/dwbriccs?driver={MS_SQL_ODBC_DRIVER.replace(" ", "+")}'
    engine = create_engine(connectionstring, echo=DATABASE_ECHO)
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
