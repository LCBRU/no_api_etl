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
        "episode",
        meta,
        Column("id", Integer, primary_key=True),
        Column("spell_id", NVARCHAR(50), index=True, nullable=False),
        Column("episode_id", NVARCHAR(50), index=True, nullable=False),
        Column("uhl_system_number", NVARCHAR(50), index=True, nullable=False),
        Column("admission_datetime", DateTime),
        Column("discharge_datetime", DateTime),
        Column("order_no_of_episode", Integer),
        Column("admission_method_code", NVARCHAR(50), index=True),
        Column("admission_method_name", NVARCHAR(100)),
        Column("admission_source_code", NVARCHAR(50), index=True),
        Column("admission_source_name", NVARCHAR(100)),
        Column("discharge_method_code", NVARCHAR(50), index=True),
        Column("discharge_method_name", NVARCHAR(100)),
        Column("speciality_code", NVARCHAR(50), index=True),
        Column("speciality_name", NVARCHAR(100)),
        Column("treatment_function_code", NVARCHAR(50), index=True),
        Column("treatment_function_name", NVARCHAR(100)),
    )
    t.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    t = Table("episode", meta, autoload=True)
    t.drop()
