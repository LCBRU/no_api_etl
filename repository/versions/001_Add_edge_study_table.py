from sqlalchemy import Table, Column, Integer, String, MetaData

meta = MetaData()

study = Table(
    'edge_study', meta,
    Column('id', Integer, primary_key=True),
    Column('title', String(500)),
)

def upgrade(migrate_engine):
    meta.bind = migrate_engine
    study.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    study.drop()
