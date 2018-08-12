#!/usr/bin/env python3

from sqlalchemy import Column, Integer, String
from api.database import Base

class EdgeStudy(Base):
    __tablename__ = 'edge_study'

    id = Column(Integer, primary_key=True)
    title = Column(String)
    site = Column(String)
    portfolio_number = Column(String)
    status = Column(String)
    type = Column(String)

    def __repr__(self):
        return "<Study(id='{}' title='{}' site='{}' portfolio_number='{}' status='{}' type='{}')>".format(
            self.id,
            self.title,
            self.site,
            self.portfolio_number,
            self.status,
            self.type,
        )


class CrfmStudy(Base):
    __tablename__ = 'crfm_study'

    id = Column(Integer, primary_key=True)
    portfolio_number = Column(String)
    title = Column(String)
    rd_number = Column(String)
    crn_number = Column(String)
    status = Column(String)

    def __repr__(self):
        return "<Study(id='{}' portfolio_number='{}' title='{}' rd_number='{}' crn_number='{}' status='{}')>".format(
            self.id,
            self.portfolio_number,
            self.title,
            self.rd_number,
            self.crn_number,
            self.status,
        )

