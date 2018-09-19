#!/usr/bin/env python3

from sqlalchemy import Column, Integer, String, Date, Float
from api.database import Base

class EdgeStudy(Base):
    __tablename__ = 'edge_study'

    id = Column(Integer, primary_key=True)
    edge_study_id = Column(Integer)
    title = Column(String)
    full_title = Column(String)
    status = Column(String)
    type = Column(String)
    chief_investigator = Column(String)
    planned_start_date = Column(Date)
    start_date = Column(Date)
    planned_end_date = Column(Date)
    end_date = Column(Date)
    disease_area = Column(String)

    local_project_reference = Column(String)
    nihr_portfolio_study_id = Column(String)
    iras_number = Column(String)
    mrec_number = Column(String)

    target_size = Column(Integer)
    actual_recruitment = Column(Integer)

    def __repr__(self):
        return ("<EdgeStudy(id='{}' edge_study_id='{}' title='{}' "
            "full_title='{}' status='{}' type='{}' chief_investigator='{}' "
            "planned_start_date='{}' start_date='{}' "
            "planned_end_date='{}' end_date='{}' "
            "local_project_reference='{}' nihr_portfolio_study_id='{}' "
            "iras_number='{}' mrec_number='{}' "
            "target_size='{}' actual_recruitment='{}')>".format(
            self.id,
            self.edge_study_id,
            self.title,
            self.full_title,
            self.status,
            self.type,
            self.chief_investigator,
            self.planned_start_date,
            self.start_date,
            self.planned_end_date,
            self.end_date,
            self.local_project_reference,
            self.nihr_portfolio_study_id,
            self.iras_number,
            self.mrec_number,
            self.target_size,
            self.actual_recruitment,
        ))


class EdgeStudySite(Base):
    __tablename__ = 'edge_study_site'

    id = Column(Integer, primary_key=True)
    edge_study_site_id = Column(Integer)
    edge_study_id = Column(Integer)
    site = Column(String)
    status = Column(String)
    site_type = Column(String)
    principal_investigator = Column(String)
    site_target_recruitment = Column(Integer)

    approval_process = Column(String)
    randd_submission_date = Column(Date)
    start_date = Column(Date)
    ssi_date = Column(Date)
    candc_assessment_required = Column(String)
    date_site_invited = Column(Date)
    date_site_selected = Column(Date)
    date_site_confirmed_by_sponsor = Column(Date)
    date_site_confirmed = Column(Date)
    non_confirmation_status = Column(String)
    date_of_non_confirmation = Column(Date)

    siv_date = Column(Date)
    open_to_recruitment_date = Column(Date)
    recruitment_start_date_date_planned = Column(Date)
    recruitment_start_date_date_actual = Column(Date)
    planned_closing_date = Column(Date)
    closed_date = Column(Date)

    first_patient_consented = Column(Date)
    first_patient_recruited = Column(Date)
    first_patient_recruited_consent_date = Column(Date)
    recruitment_clock_days = Column(Integer)
    ssi_to_first_patient_days = Column(Integer)

    estimated_annual_target = Column(Float)
    estimated_months_running = Column(Float)
    actual_recruitment = Column(Integer)
    last_patient_recruited = Column(Date)
    last_patient_referred = Column(Date)

    def __repr__(self):
        return ("<EdgeStudy(id='{}' edge_study_site_id='{}' edge_study_id='{}' site='{}' "
            "status='{}' site_type='{}' principal_investigator='{}' "
            "site_target_recruitment='{}' approval_process='{}' randd_submission_date='{}' "
            "start_date='{}' ssi_date='{}' candc_assessment_required='{}' "
            "date_site_invited='{}' date_site_selected='{}' "
            "date_site_confirmed_by_sponsor='{}' date_site_confirmed='{}' "
            "non_confirmation_status='{}' date_of_non_confirmation='{}' "
            "siv_date='{}' open_to_recruitment_date='{}' "
            "recruitment_start_date_date_planned='{}' recruitment_start_date_date_actual='{}' "
            "planned_closing_date='{}' closed_date='{}' "
            "first_patient_consented='{}' first_patient_recruited='{}' "
            "first_patient_recruited_consent_date='{}' recruitment_clock_days='{}' "
            "ssi_to_first_patient_days='{}' estimated_annual_target='{}' "
            "estimated_months_running='{}' actual_recruitment='{}' "
            "last_patient_recruited='{}' last_patient_referred='{}')>".format(
            self.id,
            self.edge_study_site_id,
            self.edge_study_id,
            self.site,
            self.status,
            self.site_type,
            self.principal_investigator,
            self.site_target_recruitment,
            self.approval_process,
            self.randd_submission_date,
            self.start_date,
            self.ssi_date,
            self.candc_assessment_required,
            self.date_site_invited,
            self.date_site_selected,
            self.date_site_confirmed_by_sponsor,
            self.date_site_confirmed,
            self.non_confirmation_status,
            self.date_of_non_confirmation,
            self.siv_date,
            self.open_to_recruitment_date,
            self.recruitment_start_date_date_planned,
            self.recruitment_start_date_date_actual,
            self.planned_closing_date,
            self.closed_date,
            self.first_patient_consented,
            self.first_patient_recruited,
            self.first_patient_recruited_consent_date,
            self.recruitment_clock_days,
            self.ssi_to_first_patient_days,
            self.estimated_annual_target,
            self.estimated_months_running,
            self.actual_recruitment,
            self.last_patient_recruited,
            self.last_patient_referred,
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
