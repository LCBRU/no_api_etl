"""Database context manager
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from sqlalchemy.ext.declarative import declarative_base
from api.environment import ETL_CENTRAL_CONNECTION_STRING

Base = declarative_base()


@contextmanager
def etl_central_session():
    try:
        engine = create_engine(ETL_CENTRAL_CONNECTION_STRING, echo=True)
        session_maker = sessionmaker(bind=engine)
        session = session_maker()
        yield session

    except Exception as e:
        session.rollback()
        raise e
    else:
        session.commit()
