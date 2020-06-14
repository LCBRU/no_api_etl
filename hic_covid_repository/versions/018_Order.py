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
        "order",
        meta,
        Column("id", Integer, primary_key=True),
        Column("order_id", NVARCHAR(50), unique=True, nullable=False),
        Column("order_key", NVARCHAR(50), unique=True, nullable=False),
        Column("uhl_system_number", NVARCHAR(50), index=True),
        Column("scheduled_datetime",DateTime, index=True),
        Column("request_datetime",DateTime, index=True),
        Column("examination_code", NVARCHAR(50)),
        Column("examination_description", NVARCHAR(100)),
        Column("snomed_code", NVARCHAR(50)),
        Column("modality", NVARCHAR(50)),
    )
    t.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    t = Table("order", meta, autoload=True)
    t.drop()
