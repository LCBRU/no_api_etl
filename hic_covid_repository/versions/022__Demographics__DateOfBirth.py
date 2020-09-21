from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    BigInteger,
    NVARCHAR,
    DateTime,
    Date,
    Boolean,
)


meta = MetaData()


def upgrade(migrate_engine):
    meta.bind = migrate_engine

    t = Table("demographics", meta, autoload=True)

    date_of_birth = Column("date_of_birth", Date)
    date_of_birth.create(t)


def downgrade(migrate_engine):
    meta.bind = migrate_engine

    t = Table("demographics", meta, autoload=True)

    t.c.date_of_birth.drop()
