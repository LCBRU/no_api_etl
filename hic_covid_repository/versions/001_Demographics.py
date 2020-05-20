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
        "demographics",
        meta,
        Column("id", Integer, primary_key=True),
        Column("participant_identifier", NVARCHAR(50), index=True, unique=True),
        Column("nhs_number", NVARCHAR(50), index=True, unique=True),
        Column("uhl_system_number", NVARCHAR(50), index=True, nullable=False, unique=True),
        Column("gp_practice", NVARCHAR(50), nullable=True),
        Column("age", Integer, nullable=True),
        Column("date_of_death", Date, nullable=True),
        Column("postcode", NVARCHAR(50), nullable=True),
        Column("sex", Integer, nullable=True),
        Column("ethnic_category", NVARCHAR(10), nullable=True),
    )
    t.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    t = Table("demographics", meta, autoload=True)
    t.drop()
