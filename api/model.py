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
    portfolio_number = Column(String)
    title = Column(String)
    rd_number = Column(String)
    crn_number = Column(String)
    status = Column(String)
    type = Column(String)

    def __repr__(self):
        return ("<CrfmStudy(id='{}' portfolio_number='{}' title='{}' "
            "rd_number='{}' crn_number='{}' status='{}')>".format(
            self.id,
            self.portfolio_number,
            self.title,
            self.rd_number,
            self.crn_number,
            self.status,
        ))

