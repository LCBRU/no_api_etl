from sqlalchemy import Table, Column, Integer, MetaData, NVARCHAR, Float, Date

meta = MetaData()

study_site = Table(
    'edge_study_site', meta,
    Column('id', Integer, primary_key=True),
    Column('edge_study_site_id', Integer),
    Column('edge_study_id', Integer),
    Column('site', NVARCHAR(500)),
    Column('status', NVARCHAR(100)),
    Column('site_type', NVARCHAR(100)),
    Column('principal_investigator', NVARCHAR(200)),
    Column('site_target_recruitment', Integer),
    Column('approval_process', NVARCHAR(100)),
    Column('randd_submission_date', Date),
    Column('start_date', Date),
    Column('ssi_date', Date),
    Column('candc_assessment_required', NVARCHAR(10)),
    Column('date_site_invited', Date),
    Column('date_site_selected', Date),
    Column('date_site_confirmed_by_sponsor', Date),
    Column('date_site_confirmed', Date),
    Column('non_confirmation_status', NVARCHAR(100)),
    Column('date_of_non_confirmation', Date),
    Column('siv_date', Date),
    Column('open_to_recruitment_date', Date),
    Column('recruitment_start_date_date_planned', Date),
    Column('recruitment_start_date_date_actual', Date),
    Column('planned_closing_date', Date),
    Column('closed_date', Date),
    Column('first_patient_consented', Date),
    Column('first_patient_recruited', Date),
    Column('first_patient_recruited_consent_date', Date),
    Column('recruitment_clock_days', Integer),
    Column('ssi_to_first_patient_days', Integer),
    Column('estimated_annual_target', Float),
    Column('estimated_months_running', Float),
    Column('actual_recruitment', Integer),
    Column('last_patient_recruited', Date),
    Column('last_patient_referred', Date),
)

def upgrade(migrate_engine):
    meta.bind = migrate_engine
    study_site.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    study_site.drop()
