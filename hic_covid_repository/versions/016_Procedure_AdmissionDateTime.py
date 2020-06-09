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

    t = Table("procedure", meta, autoload=True)

    admission_datetime = Column("admission_datetime", DateTime)
    admission_datetime.create(t)


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    t = Table("procedure", meta, autoload=True)
    t.c.admission_datetime.drop()
