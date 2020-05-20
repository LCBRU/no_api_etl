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
        "emergency",
        meta,
        Column("id", Integer, primary_key=True),
        Column("visitid", Integer, index=True, nullable=False),
        Column("uhl_system_number", NVARCHAR(50), index=True, nullable=False),
        Column("arrival_datetime", DateTime, index=True),
        Column("departure_datetime", DateTime, index=True),
        Column("arrival_mode_code", Integer, index=True),
        Column("arrival_mode_text", NVARCHAR(500)),
        Column("departure_code", Integer, index=True),
        Column("departure_text", NVARCHAR(500)),
        Column("complaint_code", Integer, index=True),
        Column("complaint_text", NVARCHAR(500)),
    )
    t.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    t = Table("emergency", meta, autoload=True)
    t.drop()
