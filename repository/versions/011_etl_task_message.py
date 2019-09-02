from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    NVARCHAR,
    ForeignKey,
    DateTime,
    UniqueConstraint,
    TEXT,
)

meta = MetaData()


def upgrade(migrate_engine):
    meta.bind = migrate_engine

    et = Table("etl_task", meta, autoload=True)

    t = Table(
        "etl_task_message",
        meta,
        Column("id", Integer, primary_key=True),
        Column("etl_task_id", Integer, ForeignKey(et.c.id), index=True, nullable=False),
        Column("message_datetime", DateTime, nullable=False),
        Column("message_type", NVARCHAR(50), nullable=False),
        Column("message", TEXT, nullable=False),
    )
    t.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    t = Table("etl_task_message", meta, autoload=True)
    t.drop()
