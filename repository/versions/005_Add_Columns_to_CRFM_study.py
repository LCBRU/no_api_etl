from sqlalchemy import Table, MetaData, Column, NVARCHAR, Date, Integer

def upgrade(migrate_engine):
    meta = MetaData(bind=migrate_engine)
    study = Table('crfm_study', meta, autoload=True)

    col_study_number = Column('study_number', NVARCHAR(50))
    col_study_number.create(study)

    col_ethics_number = Column('ethics_number', NVARCHAR(50))
    col_ethics_number.create(study)

    col_clinical_trial_gov = Column('clinical_trial_gov', NVARCHAR(50))
    col_clinical_trial_gov.create(study)

    col_isrctn = Column('isrctn', NVARCHAR(50))
    col_isrctn.create(study)

    col_iras_number = Column('iras_number', NVARCHAR(50))
    col_iras_number.create(study)

    col_who = Column('who', NVARCHAR(50))
    col_who.create(study)

    col_eudract = Column('eudract', NVARCHAR(50))
    col_eudract.create(study)

    study.c.crn_number.alter(name='nihr_crn_number')
    study.c.portfolio_number.alter(name='protocol_number')

def downgrade(migrate_engine):
    meta = MetaData(bind=migrate_engine)
    study = Table('crfm_study', meta, autoload=True)
    study.c.study_number.drop()
    study.c.ethics_number.drop()
    study.c.isrctn.drop()
    study.c.iras_number.drop()
    study.c.who.drop()
    study.c.eudract.drop()
    study.c.nihr_crn_number.alter(name='crn_number')
    study.c.protocol_number.alter(name='portfolio_number')
