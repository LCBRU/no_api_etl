from sqlalchemy import Table, MetaData, Column, NVARCHAR, Integer

def upgrade(migrate_engine):
    meta = MetaData(bind=migrate_engine)
    study = Table('edge_study', meta, autoload=True)

    study.c.chief_investigator.drop()
    chief_investigator = Column('chief_investigator', NVARCHAR(200))
    chief_investigator.create(study)

    study.c.disease_area.drop()
    disease_area = Column('disease_area', NVARCHAR(200))
    disease_area.create(study)

    study.c.mrec_number.drop()
    mrec_number = Column('mrec_number', NVARCHAR(200))
    mrec_number.create(study)


def downgrade(migrate_engine):
    meta = MetaData(bind=migrate_engine)
    study = Table('edge_study', meta, autoload=True)

    study.c.chief_investigator.drop()
    chief_investigator = Column('chief_investigator', Integer)
    chief_investigator.create(study)

    study.c.disease_area.drop()
    disease_area = Column('disease_area', Integer)
    disease_area.create(study)

    study.c.mrec_number.drop()
    mrec_number = Column('mrec_number', Integer)
    mrec_number.create(study)
