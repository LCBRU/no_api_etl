from sqlalchemy import Table, MetaData, Column, NVARCHAR, Date, Integer

def upgrade(migrate_engine):
    meta = MetaData(bind=migrate_engine)
    study = Table('edge_study', meta, autoload=True)
    col_designs = Column('designs', NVARCHAR(200))
    col_designs.create(study)
    col_site_type = Column('site_type', NVARCHAR(100))
    col_site_type.create(study)
    col_recruitment_start_date = Column('recruitment_start_date', Date)
    col_recruitment_start_date.create(study)
    col_recruitment_end_date = Column('recruitment_end_date', Date)
    col_recruitment_end_date.create(study)
    col_recruitment_target = Column('recruitment_target', Integer)
    col_recruitment_target.create(study)
    col_recruitment_so_far = Column('recruitment_so_far', Integer)
    col_recruitment_so_far.create(study)

def downgrade(migrate_engine):
    meta = MetaData(bind=migrate_engine)
    study = Table('edge_study', meta, autoload=True)
    study.c.designs.drop()
    study.c.site_type.drop()
    study.c.recruitment_start_date.drop()
    study.c.recruitment_end_date.drop()
    study.c.col_recruitment_target.drop()
    study.c.col_recruitment_so_far.drop()
