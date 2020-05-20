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
        "blood_test",
        meta,
        Column("id", Integer, primary_key=True),
        Column("test_id", Integer, index=True, nullable=False),
        Column("uhl_system_number", NVARCHAR(50), index=True, nullable=False),
        Column("test_code", NVARCHAR(50)),
        Column("test_name", NVARCHAR(50), index=True),
        Column("result", NVARCHAR(100)),
        Column("result_expansion", NVARCHAR(100)),
        Column("result_units", NVARCHAR(50)),
        Column("sample_collected_datetime", DateTime),
        Column("result_datetime", DateTime),
        Column("lower_range", DECIMAL),
        Column("higher_range", DECIMAL),
    )
    t.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    t = Table("blood_test", meta, autoload=True)
    t.drop()
