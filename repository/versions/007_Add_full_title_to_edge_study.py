from sqlalchemy import Table, MetaData, Column, NVARCHAR

def upgrade(migrate_engine):
    meta = MetaData(bind=migrate_engine)
    study = Table('edge_study', meta, autoload=True)

    full_title = Column('full_title', NVARCHAR(500))
    full_title.create(study)

def downgrade(migrate_engine):
    meta = MetaData(bind=migrate_engine)
    study = Table('edge_study', meta, autoload=True)
    study.c.full_title.drop()
