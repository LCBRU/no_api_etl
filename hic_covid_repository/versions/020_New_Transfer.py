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
        Column("transfer_type", NVARCHAR(50), index=True, nullable=False),
        Column("uhl_system_number", NVARCHAR(50), index=True, nullable=False),
        Column("transfer_datetime", DateTime),
        Column("ward_code", NVARCHAR(50), index=True),
        Column("ward_name", NVARCHAR(100)),
        Column("hospital", NVARCHAR(100)),
    )
    t.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    t = Table("transfer", meta, autoload=True)
    t.drop()
