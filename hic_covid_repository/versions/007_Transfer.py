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
        "transfer",
        meta,
        Column("id", Integer, primary_key=True),
        Column("spell_id", NVARCHAR(50), index=True, nullable=False),
        Column("transfer_id", NVARCHAR(50), index=True, nullable=False),
        Column("uhl_system_number", NVARCHAR(50), index=True, nullable=False),
        Column("transfer_datetime", DateTime),
        Column("from_bed", NVARCHAR(50)),
        Column("from_ward_code", NVARCHAR(50), index=True),
        Column("from_ward_name", NVARCHAR(100)),
        Column("from_hospital", NVARCHAR(100)),
        Column("to_bed", NVARCHAR(50)),
        Column("to_ward_code", NVARCHAR(50), index=True),
        Column("to_ward_name", NVARCHAR(100)),
        Column("to_hospital", NVARCHAR(100)),
    )
    t.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    t = Table("transfer", meta, autoload=True)
    t.drop()
