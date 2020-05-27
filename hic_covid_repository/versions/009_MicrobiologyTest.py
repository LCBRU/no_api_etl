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
        "microbiology_test",
        meta,
        Column("id", Integer, primary_key=True),
        Column("test_id", Integer, index=True, nullable=False),
        Column("uhl_system_number", NVARCHAR(50), index=True, nullable=False),
        Column("order_code", NVARCHAR(50)),
        Column("order_name", NVARCHAR(50), index=True),
        Column("test_code", NVARCHAR(50)),
        Column("test_name", NVARCHAR(50), index=True),
        Column("organism", NVARCHAR(100)),
        Column("result", NVARCHAR(100)),
        Column("sample_collected_datetime", DateTime),
        Column("sample_received_datetime", DateTime),
        Column("result_datetime", DateTime),
        Column("specimen_site", NVARCHAR(100)),
    )
    t.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    t = Table("microbiology_test", meta, autoload=True)
    t.drop()
