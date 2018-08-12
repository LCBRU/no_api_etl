from sqlalchemy import Table, MetaData, String, Column


def upgrade(migrate_engine):
    meta = MetaData(bind=migrate_engine)
    study = Table('edge_study', meta, autoload=True)
    col_site = Column('site', String(500))
    col_site.create(study)
    col_portfolio_number = Column('portfolio_number', String(500))
    col_portfolio_number.create(study)
    col_status = Column('status', String(50))
    col_status.create(study)
    col_type = Column('type', String(50))
    col_type.create(study)

def downgrade(migrate_engine):
    meta = MetaData(bind=migrate_engine)
    study = Table('edge_study', meta, autoload=True)
    study.c.site.drop()
    study.c.portfolio_number.drop()
    study.c.status.drop()
    study.c.type.drop()
