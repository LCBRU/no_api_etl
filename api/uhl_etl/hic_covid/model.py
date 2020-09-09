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
    admission_datetime = Column(DateTime)
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
    admission_datetime = Column(DateTime)
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
    transfer_type = Column(String)
    uhl_system_number = Column(String)
    transfer_datetime = Column(DateTime)
    ward_code = Column(String)
    ward_name = Column(String)
    hospital = Column(String)


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
    receive_datetime = Column(DateTime)
    lower_range = Column(DECIMAL)
    higher_range = Column(DECIMAL)


class MicrobiologyTest(Base):
    __tablename__ = 'microbiology_test'

    id = Column(Integer, primary_key=True)
    test_id = Column(Integer)
    uhl_system_number = Column(String)
    order_code = Column(String)
    order_name = Column(String)
    test_code = Column(String)
    test_name = Column(String)
    organism = Column(String)
    result = Column(String)
    sample_collected_datetime = Column(DateTime)
    sample_received_datetime = Column(DateTime)
    result_datetime = Column(DateTime)
    specimen_site = Column(String)


class Prescribing(Base):
    __tablename__ = 'prescribing'

    id = Column(Integer, primary_key=True)
    order_id = Column(Integer)
    uhl_system_number = Column(String)
    method_name = Column(String)
    order_type = Column(Integer)
    medication_name = Column(String)
    min_dose = Column(String)
    max_does = Column(String)
    frequency = Column(String)
    form = Column(String)
    does_units = Column(String)
    route = Column(String)
    ordered_datetime = Column(DateTime)


class Administration(Base):
    __tablename__ = 'administration'

    id = Column(Integer, primary_key=True)
    administration_id = Column(Integer)
    uhl_system_number = Column(String)
    administration_datetime = Column(DateTime)
    medication_name = Column(String)
    dose_id = Column(String)
    dose = Column(String)
    dose_unit = Column(String)
    form_name = Column(String)
    route_name = Column(String)


class Observation(Base):
    __tablename__ = 'observation'

    id = Column(Integer, primary_key=True)
    observation_id = Column(Integer)
    uhl_system_number = Column(String)
    observation_datetime = Column(DateTime)
    observation_name = Column(String)
    observation_value = Column(String)
    observation_ews = Column(String)
    observation_units = Column(String)


class CriticalCarePeriod(Base):
    __tablename__ = 'critical_care_period'

    id = Column(Integer, primary_key=True)
    ccp_id = Column(Integer)
    uhl_system_number = Column(String)
    local_identifier = Column(String)
    treatment_function_code = Column(String)
    treatment_function_name = Column(String)
    start_datetime = Column(DateTime)
    location = Column(String)
    basic_respiratory_support_days = Column(String)
    advanced_respiratory_support_days = Column(String)
    basic_cardiovascular_support_days = Column(String)
    advanced_cardiovascular_support_days = Column(String)
    renal_support_days = Column(String)
    neurological_support_days = Column(String)
    dermatological_support_days = Column(String)
    liver_support_days = Column(String)
    critical_care_level_2_days = Column(String)
    critical_care_level_3_days = Column(String)
    discharge_datetime = Column(DateTime)


class Order(Base):
    __tablename__ = 'order'

    id = Column(Integer, primary_key=True)
    order_id = Column(String)
    order_key = Column(String)
    uhl_system_number = Column(String)
    scheduled_datetime = Column(DateTime)
    request_datetime = Column(DateTime)
    examination_code = Column(String)
    examination_description = Column(String)
    snomed_code = Column(String)
    modality = Column(String)


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