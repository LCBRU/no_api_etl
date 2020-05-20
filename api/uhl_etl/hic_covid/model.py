#!/usr/bin/env python3

from sqlalchemy import (
    Column,
    Integer,
    String,
    Date,
    DateTime,
    Float,
    Boolean,
    ForeignKey,
    create_engine,
    MetaData,
    DECIMAL,
)
from sqlalchemy.orm import relationship, sessionmaker
from contextlib import contextmanager
from sqlalchemy.ext.declarative import declarative_base
from api.environment import (
    DATABASE_ECHO,
    HIC_COVID_CONNECTION_STRING,
)


hic_covid_meta = MetaData()
Base = declarative_base(metadata=hic_covid_meta)


class Demographics(Base):
    __tablename__ = 'demographics'

    id = Column(Integer, primary_key=True)
    participant_identifier = Column(String)
    nhs_number = Column(String)
    uhl_system_number = Column(String)
    gp_practice = Column(String)
    age = Column(Integer)
    date_of_death = Column(Date)
    postcode = Column(String)
    sex = Column(Integer)
    ethnic_category = Column(String)


class Virology(Base):
    __tablename__ = 'virology'

    id = Column(Integer, primary_key=True)
    uhl_system_number = Column(String)
    laboratory_code = Column(String)
    test_id = Column(Integer)
    order_code = Column(String)
    order_name = Column(String)
    test_code = Column(String)
    test_name = Column(String)
    organism = Column(String)
    test_result = Column(String)
    sample_collected_date_time = Column(DateTime)
    sample_received_date_time = Column(DateTime)
    sample_available_date_time = Column(DateTime)
    order_status = Column(String)


class Emergency(Base):
    __tablename__ = 'emergency'

    id = Column(Integer, primary_key=True)
    visitid = Column(Integer)
    uhl_system_number = Column(String)
    arrival_datetime = Column(DateTime)
    departure_datetime = Column(DateTime)
    arrival_mode_code = Column(Integer)
    arrival_mode_text = Column(String)
    departure_code = Column(Integer)
    departure_text = Column(String)
    complaint_code = Column(Integer)
    complaint_text = Column(String)


class Episode(Base):
    __tablename__ = 'episode'

    id = Column(Integer, primary_key=True)
    episode_id = Column(String)
    spell_id = Column(String)
    uhl_system_number = Column(String)
    admission_datetime = Column(DateTime)
    discharge_datetime = Column(DateTime)
    order_no_of_episode = Column(Integer)
    admission_method_code = Column(String)
    admission_method_name = Column(String)
    admission_source_code = Column(String)
    admission_source_name = Column(String)
    discharge_method_code = Column(String)
    discharge_method_name = Column(String)
    speciality_code = Column(String)
    speciality_name = Column(String)
    treatment_function_code = Column(String)
    treatment_function_name = Column(String)


class Diagnosis(Base):
    __tablename__ = 'diagnosis'

    id = Column(Integer, primary_key=True)
    spell_id = Column(String)
    episode_id = Column(String)
    diagnosis_id = Column(String)
    uhl_system_number = Column(String)
    diagnosis_number = Column(Integer)
    diagnosis_code = Column(String)
    diagnosis_name = Column(String)


class Procedure(Base):
    __tablename__ = 'procedure'

    id = Column(Integer, primary_key=True)
    spell_id = Column(String)
    episode_id = Column(String)
    procedure_id = Column(String)
    uhl_system_number = Column(String)
    procedure_number = Column(Integer)
    procedure_code = Column(String)
    procedure_name = Column(String)


class Transfer(Base):
    __tablename__ = 'transfer'

    id = Column(Integer, primary_key=True)
    spell_id = Column(String)
    transfer_id = Column(String)
    uhl_system_number = Column(String)
    transfer_datetime = Column(DateTime)
    from_bed = Column(String)
    from_ward_code = Column(String)
    from_ward_name = Column(String)
    from_hospital = Column(String)
    to_bed = Column(String)
    to_ward_code = Column(String)
    to_ward_name = Column(String)
    to_hospital = Column(String)


class BloodTest(Base):
    __tablename__ = 'blood_test'

    id = Column(Integer, primary_key=True)
    test_id = Column(Integer)
    uhl_system_number = Column(String)
    test_code = Column(String)
    test_name = Column(String)
    result = Column(String)
    result_expansion = Column(String)
    result_units = Column(String)
    sample_collected_datetime = Column(DateTime)
    result_datetime = Column(DateTime)
    lower_range = Column(DECIMAL)
    higher_range = Column(DECIMAL)


@contextmanager
def hic_covid_session():
    try:
        engine = create_engine(HIC_COVID_CONNECTION_STRING, echo=DATABASE_ECHO)
        hic_covid_meta.bind = engine
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