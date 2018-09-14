from sqlalchemy import Table, MetaData, Column, NVARCHAR, Date, Integer

def upgrade(migrate_engine):
    meta = MetaData(bind=migrate_engine)
    study = Table('edge_study', meta, autoload=True)

    study.c.portfolio_number.alter(name='nihr_portfolio_study_id')

    edge_study_id = Column('edge_study_id', Integer)
    edge_study_id.create(study)
    chief_investigator = Column('chief_investigator', Integer)
    chief_investigator.create(study)
    planned_start_date = Column('planned_start_date', Date)
    planned_start_date.create(study)
    start_date = Column('start_date', Date)
    start_date.create(study)
    planned_end_date = Column('planned_end_date', Date)
    planned_end_date.create(study)
    end_date = Column('end_date', Date)
    end_date.create(study)
    disease_area = Column('disease_area', Integer)
    disease_area.create(study)

    local_project_reference = Column('local_project_reference', Integer)
    local_project_reference.create(study)
    iras_number = Column('iras_number', Integer)
    iras_number.create(study)
    mrec_number = Column('mrec_number', Integer)
    mrec_number.create(study)

    target_size = Column('target_size', Integer)
    target_size.create(study)
    actual_recruitment = Column('actual_recruitment', Integer)
    actual_recruitment.create(study)

def downgrade(migrate_engine):
    meta = MetaData(bind=migrate_engine)
    study = Table('edge_study', meta, autoload=True)
    study.c.edge_id.drop()
    study.c.chief_investigator.drop()
    study.c.planned_start_date.drop()
    study.c.start_date.drop()
    study.c.planned_end_date.drop()
    study.c.end_date.drop()
    study.c.disease_area.drop()
    study.c.local_project_reference.drop()
    study.c.iras_number.drop()
    study.c.mrec_number.drop()
    study.c.target_size.drop()
    study.c.actual_recruitment.drop()

    study.c.nihr_portfolio_study_id.alter(name='portfolio_number')
