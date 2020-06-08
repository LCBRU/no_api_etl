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
        "prescribing",
        meta,
        Column("id", Integer, primary_key=True),
        Column("order_id", NVARCHAR(50), index=True, nullable=False),
        Column("uhl_system_number", NVARCHAR(50), index=True, nullable=False),
        Column("method_name", NVARCHAR(100)),
        Column("order_type", Integer),
        Column("medication_name", NVARCHAR(100)),
        Column("min_dose", NVARCHAR(50)),
        Column("max_does", NVARCHAR(50)),
        Column("frequency", NVARCHAR(50)),
        Column("form", NVARCHAR(50)),
        Column("does_units", NVARCHAR(50)),
        Column("route", NVARCHAR(50)),
        Column("ordered_datetime", DateTime),
    )
    t.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    t = Table("prescribing", meta, autoload=True)
    t.drop()
