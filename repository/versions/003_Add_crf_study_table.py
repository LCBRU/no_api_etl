from sqlalchemy import Table, Column, Integer, String, MetaData

meta = MetaData()

study = Table(
    'crfm_study', meta,
    Column('id', Integer, primary_key=True),
    Column('portfolio_number', String(100)),
    Column('title', String(500)),
    Column('rd_number', String(100)),
    Column('crn_number', String(100)),
    Column('status', String(50)),
)

def upgrade(migrate_engine):
    meta.bind = migrate_engine
    study.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    study.drop()
