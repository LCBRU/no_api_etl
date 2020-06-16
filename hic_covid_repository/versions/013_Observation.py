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
        "observation",
        meta,
        Column("id", Integer, primary_key=True),
        Column("observation_id", Integer, index=True, nullable=False),
        Column("uhl_system_number", NVARCHAR(50), index=True, nullable=False),
        Column("observation_datetime", DateTime),
        Column("observation_name", NVARCHAR(100)),
        Column("observation_value", NVARCHAR(50)),
        Column("observation_ews", NVARCHAR(50)),
        Column("observation_units", NVARCHAR(50)),
    )
    t.create()

    cons = UniqueConstraint(t.c.observation_id, t.c.observation_name, name='ix_observation__id__name')
    cons.create()



def downgrade(migrate_engine):
    meta.bind = migrate_engine
    t = Table("observation", meta, autoload=True)
    t.drop()
