from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    NVARCHAR,
    DateTime,
    Date,
    Boolean,
    DECIMAL,
)


meta = MetaData()


def upgrade(migrate_engine):
    meta.bind = migrate_engine

    t = Table(
        "administration",
        meta,
        Column("id", Integer, primary_key=True),
        Column("administration_id", NVARCHAR(50), index=True, nullable=False),
        Column("uhl_system_number", NVARCHAR(50), index=True, nullable=False),
        Column("administration_datetime", DateTime),
        Column("medication_name", NVARCHAR(100)),
        Column("dose_id", NVARCHAR(50)),
        Column("dose", NVARCHAR(50)),
        Column("dose_unit", NVARCHAR(50)),
        Column("form_name", NVARCHAR(50)),
        Column("route_name", NVARCHAR(50)),
    )
    t.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    t = Table("administration", meta, autoload=True)
    t.drop()
