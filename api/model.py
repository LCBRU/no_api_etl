#!/usr/bin/env python3

from sqlalchemy import Column, Integer, String, Date, ForeignKey
from sqlalchemy.orm import relationship
from api.database import Base


class CrfmStudy(Base):
    __tablename__ = 'crfm_study'

    id = Column(Integer, primary_key=True)
    study_number = Column(String)
    protocol_number = Column(String)
    title = Column(String)
    ethics_number = Column(String)
    clinical_trial_gov = Column(String)
    isrctn = Column(String)
    iras_number = Column(String)
    nihr_crn_number = Column(String)
    rd_number = Column(String)
    who = Column(String)
    eudract = Column(String)
    status = Column(String)

    def __repr__(self):
        return ("<CrfmStudy(id='{}' study_number='{}' title='{}' "
            "ethics_number='{}' clinical_trial_gov='{}' isrctn='{}' "
            "iras_number='{}' nihr_crn_number='{}' rd_number='{}' "
            "who='{}' protocol_number='{}' eudract='{}' status='{}')>".format(
            self.id,
            self.study_number,
            self.title,
            self.ethics_number,
            self.clinical_trial_gov,
            self.isrctn,
            self.iras_number,
            self.nihr_crn_number,
            self.rd_number,
            self.who,
            self.protocol_number,
            self.eudract,
            self.status,
        ))


class EtlTask(Base):
    __tablename__ = 'etl_task'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    start_datetime = Column(Date)
    end_datetime = Column(Date)


class EtlTaskMessage(Base):
    __tablename__ = 'etl_task_message'

    id = Column(Integer, primary_key=True)
    etl_task_id = Column(Integer, ForeignKey(EtlTask.id))
    etl_task = relationship(EtlTask)
    message_datetime = Column(Date)
    message_type = Column(String)
    message = Column(String)
    attachment = Column(String)
