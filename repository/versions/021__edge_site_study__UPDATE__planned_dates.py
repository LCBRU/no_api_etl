from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Date,
)
meta = MetaData()


def upgrade(migrate_engine):
    meta.bind = migrate_engine

    t = Table("edge_site_study", meta, autoload=True)

    planned_start_date = Column("planned_start_date", Date)
    planned_start_date.create(t)

    planned_end_date = Column("planned_end_date", Date)
    planned_end_date.create(t)


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    t = Table("edge_site_study", meta, autoload=True)

    t.c.planned_start_date.drop()
    t.c.planned_end_date.drop()
