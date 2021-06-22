from sqlalchemy import (
    MetaData,
    Table,
)

meta = MetaData()


def upgrade(migrate_engine):
    meta.bind = migrate_engine

    t = Table("edge_annual_report", meta, autoload=True)
    t.drop()

def downgrade(migrate_engine):
    pass
