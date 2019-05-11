from sqlalchemy import Table, MetaData, Column, NVARCHAR, Boolean

def upgrade(migrate_engine):
    meta = MetaData(bind=migrate_engine)
    study = Table('edge_study', meta, autoload=True)

    is_uhl_lead_centre = Column('is_uhl_lead_centre', Boolean)
    is_uhl_lead_centre.create(study)

    primary_clinical_management_areas = Column('primary_clinical_management_areas', NVARCHAR(200))
    primary_clinical_management_areas.create(study)

def downgrade(migrate_engine):
    meta = MetaData(bind=migrate_engine)
    study = Table('edge_study', meta, autoload=True)
    study.c.is_uhl_lead_centre.drop()
    study.c.primary_clinical_management_areas.drop()
