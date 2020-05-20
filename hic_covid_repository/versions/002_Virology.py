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
        "virology",
        meta,
        Column("id", Integer, primary_key=True),
        Column("uhl_system_number", NVARCHAR(50), index=True, nullable=False),
        Column("test_id", Integer, nullable=False, index=True, unique=True),
        Column("laboratory_code", NVARCHAR(50)),
        Column("order_code", NVARCHAR(50)),
        Column("order_name", NVARCHAR(50)),
        Column("test_code", NVARCHAR(50)),
        Column("test_name", NVARCHAR(50)),
        Column("organism", NVARCHAR(50)),
        Column("test_result", NVARCHAR(50)),
        Column("sample_collected_date_time", DateTime),
        Column("sample_received_date_time", DateTime),
        Column("sample_available_date_time", DateTime),
        Column("order_status", NVARCHAR(50)),
    )
    t.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    t = Table("virology", meta, autoload=True)
    t.drop()
