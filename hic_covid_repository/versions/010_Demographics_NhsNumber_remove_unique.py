from sqlalchemy import (
    MetaData,
    Index,
    Table,
)
from migrate.changeset.constraint import UniqueConstraint


meta = MetaData()


def upgrade(migrate_engine):
    meta.bind = migrate_engine

    t = Table("demographics", meta, autoload=True)

    cons = UniqueConstraint(t.c.nhs_number, name='ix_demographics_nhs_number')
    cons.drop()

    idx = Index('ix_demographics_nhs_number', t.c.nhs_number)
    idx.create(migrate_engine)


def downgrade(migrate_engine):
    meta.bind = migrate_engine
