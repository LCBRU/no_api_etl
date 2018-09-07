#!/usr/bin/env python3

from sqlalchemy import Column, Integer, String, Date
from api.database import Base

class EdgeStudy(Base):
    __tablename__ = 'edge_study'

    id = Column(Integer, primary_key=True)
    title = Column(String)
    site = Column(String)
    portfolio_number = Column(String)
    status = Column(String)
    type = Column(String)
    designs = Column(String)
    site_type = Column(String)
    recruitment_start_date = Column(Date)
    recruitment_end_date = Column(Date)
    recruitment_target = Column(Integer)
    recruitment_so_far = Column(Integer)

    def __repr__(self):
        return ("<EdgeStudy(id='{}' title='{}' site='{}' "
            "portfolio_number='{}' status='{}' type='{}' "
            "designs='{}' sit_type='{}' recruitment_start_date='{}' "
            "recruitment_end_date='{}' recruitment_target='{}' "
            "recruitment_so_far='{}')>".format(
            self.id,
            self.title,
            self.site,
            self.portfolio_number,
            self.status,
            self.type,
            self.designs,
            self.site_type,
            self.recruitment_start_date,
            self.recruitment_end_date,
            self.recruitment_target,
            self.recruitment_so_far,
        ))


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
