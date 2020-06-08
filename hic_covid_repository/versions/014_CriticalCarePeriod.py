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
from migrate.changeset.constraint import UniqueConstraint


meta = MetaData()


def upgrade(migrate_engine):
    meta.bind = migrate_engine

    t = Table(
        "critical_care_period",
        meta,
        Column("id", Integer, primary_key=True),
        Column("ccp_id", NVARCHAR(50), index=True, nullable=False),
        Column("uhl_system_number", NVARCHAR(50), index=True, nullable=False),
        Column("local_identifier", NVARCHAR(50)),
        Column("treatment_function_code", NVARCHAR(100)),
        Column("treatment_function_name", NVARCHAR(50)),
        Column("start_datetime", DateTime),
        Column("location", NVARCHAR(50)),
        Column("basic_respiratory_support_days", NVARCHAR(50)),
        Column("advanced_respiratory_support_days", NVARCHAR(50)),
        Column("basic_cardiovascular_support_days", NVARCHAR(50)),
        Column("advanced_cardiovascular_support_days", NVARCHAR(50)),
        Column("renal_support_days", NVARCHAR(50)),
        Column("neurological_support_days", NVARCHAR(50)),
        Column("dermatological_support_days", NVARCHAR(50)),
        Column("liver_support_days", NVARCHAR(50)),
        Column("critical_care_level_2_days", NVARCHAR(50)),
        Column("critical_care_level_3_days", NVARCHAR(50)),
        Column("discharge_datetime", DateTime),
    )
    t.create()



def downgrade(migrate_engine):
    meta.bind = migrate_engine
    t = Table("critical_care_period", meta, autoload=True)
    t.drop()
