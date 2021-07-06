from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    NVARCHAR,
    BOOLEAN,
    DateTime,
    Date,
)
from sqlalchemy.sql.sqltypes import Boolean

meta = MetaData()


def upgrade(migrate_engine):
    meta.bind = migrate_engine

    t = Table("edge_site_study", meta, autoload=True)

    # Drop columns that are no longer required
    t.c.research_theme.drop()
    t.c.start_date.drop()
    t.c.project_site_date_open_to_recruitment.drop()
    t.c.end_date.drop()
    t.c.recruitment_end_date.drop()
    t.c.recruited_total.drop()

    # Add new columns
    mrec_number = Column("mrec_number", NVARCHAR(50))
    mrec_number.create(t)
    iras_number = Column("iras_number", Integer)
    iras_number.create(t)
    project_full_title = Column("project_full_title", NVARCHAR(500))
    project_full_title.create(t)
    project_phase = Column("project_phase", NVARCHAR(100))
    project_phase.create(t)
    primary_clinical_management_areas = Column("primary_clinical_management_areas", NVARCHAR(100))
    primary_clinical_management_areas.create(t)
    project_site_status = Column("project_site_status", NVARCHAR(100))
    project_site_status.create(t)
    project_site_rand_submission_date = Column("project_site_rand_submission_date", Date)
    project_site_rand_submission_date.create(t)
    project_site_date_site_confirmed = Column("project_site_date_site_confirmed", Date)
    project_site_date_site_confirmed.create(t)
    project_site_estimated_annual_target = Column("project_site_estimated_annual_target", Integer)
    project_site_estimated_annual_target.create(t)
    project_site_lead_nurses = Column("project_site_lead_nurses", NVARCHAR(500))
    project_site_lead_nurses.create(t)
    project_site_name = Column("project_site_name", NVARCHAR(500))
    project_site_name.create(t)
    nihr_portfolio_study_id = Column("nihr_portfolio_study_id", Integer)
    nihr_portfolio_study_id.create(t)
    pi_orcidid = Column("pi_orcidid", NVARCHAR(100))
    pi_orcidid.create(t)
    is_uhl_lead_centre = Column("is_uhl_lead_centre", Boolean)
    is_uhl_lead_centre.create(t)
    lead_centre_name_if_not_uhl = Column("lead_centre_name_if_not_uhl", NVARCHAR(500))
    lead_centre_name_if_not_uhl.create(t)
    cro_cra_used = Column("cro_cra_used", Boolean)
    cro_cra_used.create(t)
    name_of_cro_cra_company_used = Column("name_of_cro_cra_company_used", NVARCHAR(500))
    name_of_cro_cra_company_used.create(t)
    study_category = Column("study_category", NVARCHAR(500))
    study_category.create(t)
    randomised_name = Column("randomised_name", NVARCHAR(500))
    randomised_name.create(t)
    name_of_brc_involved = Column("name_of_brc_involved", NVARCHAR(500))
    name_of_brc_involved.create(t)


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    t = Table("edge_site_study", meta, autoload=True)

    # Drop new fields
    t.c.mrec_number.drop()
    t.c.iras_number.drop()
    t.c.project_full_title.drop()
    t.c.project_phase.drop()
    t.c.primary_clinical_management_areas.drop()
    t.c.project_site_status.drop()
    t.c.project_site_rand_submission_date.drop()
    t.c.project_site_date_site_confirmed.drop()
    t.c.project_site_estimated_annual_target.drop()
    t.c.project_site_lead_nurses.drop()
    t.c.project_site_name.drop()
    t.c.nihr_portfolio_study_id.drop()
    t.c.pi_orcidid.drop()
    t.c.is_uhl_lead_centre.drop()
    t.c.lead_centre_name_if_not_uhl.drop()
    t.c.cro_cra_used.drop()
    t.c.name_of_cro_cra_company_used.drop()
    t.c.study_category.drop()
    t.c.randomised_name.drop()
    t.c.name_of_brc_involved.drop()

    # Recreate fields
    research_theme = Column("research_theme", NVARCHAR(500))
    research_theme.create(t)
    start_date = Column("start_date", DateTime)
    start_date.create(t)
    project_site_date_open_to_recruitment = Column("project_site_date_open_to_recruitment", DateTime)
    project_site_date_open_to_recruitment.create(t)
    end_date = Column("end_date", DateTime)
    end_date.create(t)
    recruitment_end_date = Column("recruitment_end_date", DateTime)
    recruitment_end_date.create(t)
    recruited_total = Column("recruited_total", Integer)
    recruited_total.create(t)
