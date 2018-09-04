from sqlalchemy import Table, Column, Integer, MetaData, NVARCHAR

meta = MetaData()

study = Table(
    'edge_study', meta,
    Column('id', Integer, primary_key=True),
    Column('title', NVARCHAR(500)),
)

def upgrade(migrate_engine):
    meta.bind = migrate_engine
    study.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    study.drop()
