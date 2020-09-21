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

    t = Table("emergency", meta, autoload=True)

    t.c.arrival_mode_code.drop()
    t.c.departure_code.drop()
    t.c.complaint_code.drop()

    arrival_mode_code = Column("arrival_mode_code", BigInteger)
    arrival_mode_code.create(t)
    departure_code = Column("departure_code", BigInteger)
    departure_code.create(t)
    complaint_code = Column("complaint_code", BigInteger)
    complaint_code.create(t)


def downgrade(migrate_engine):
    meta.bind = migrate_engine

    t = Table("emergency", meta, autoload=True)

    t.c.arrival_mode_code.drop()
    t.c.departure_code.drop()
    t.c.complaint_code.drop()

    arrival_mode_code = Column("arrival_mode_code", Integer)
    arrival_mode_code.create(t)
    departure_code = Column("departure_code", Integer)
    departure_code.create(t)
    complaint_code = Column("complaint_code", Integer)
    complaint_code.create(t)
