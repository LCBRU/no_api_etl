from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    NVARCHAR,
    DateTime,
    Date,
    Boolean,
)


meta = MetaData()


def upgrade(migrate_engine):
    meta.bind = migrate_engine

    t = Table(
        "diagnosis",
        meta,
        Column("id", Integer, primary_key=True),
        Column("spell_id", NVARCHAR(50), index=True, nullable=False),
        Column("episode_id", NVARCHAR(50), index=True, nullable=False),
        Column("diagnosis_id", NVARCHAR(50), index=True, nullable=False),
        Column("uhl_system_number", NVARCHAR(50), index=True, nullable=False),
        Column("diagnosis_number", Integer),
        Column("diagnosis_code", NVARCHAR(50), index=True),
        Column("diagnosis_name", NVARCHAR(100)),
    )
    t.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    t = Table("diagnosis", meta, autoload=True)
    t.drop()
