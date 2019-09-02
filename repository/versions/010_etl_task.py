from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    NVARCHAR,
    DateTime,
)

meta = MetaData()


def upgrade(migrate_engine):
    meta.bind = migrate_engine

    t = Table(
        "etl_task",
        meta,
        Column("id", Integer, primary_key=True),
        Column("name", NVARCHAR(500), nullable=False),
        Column("start_datetime", DateTime, nullable=False),
        Column("end_datetime", DateTime, nullable=True),
    )
    t.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    t = Table("etl_task", meta, autoload=True)
    t.drop()
