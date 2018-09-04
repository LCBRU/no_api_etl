from sqlalchemy import Table, Column, Integer, MetaData, NVARCHAR

meta = MetaData()

study = Table(
    'crfm_study', meta,
    Column('id', Integer, primary_key=True),
    Column('portfolio_number', NVARCHAR(100)),
    Column('title', NVARCHAR(500)),
    Column('rd_number', NVARCHAR(100)),
    Column('crn_number', NVARCHAR(100)),
    Column('status', NVARCHAR(50)),
)

def upgrade(migrate_engine):
    meta.bind = migrate_engine
    study.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    study.drop()
