from sqlalchemy import (
    MetaData,
    Table,
    Column,
    NVARCHAR,
)
meta = MetaData()


def upgrade(migrate_engine):
    meta.bind = migrate_engine

    t = Table("edge_site_study", meta, autoload=True)

    t.c.principle_investigator.drop()

    principal_investigator = Column("principal_investigator", NVARCHAR(500))
    principal_investigator.create(t)

def downgrade(migrate_engine):
    meta.bind = migrate_engine
    t = Table("edge_site_study", meta, autoload=True)

    t.c.principal_investigator.drop()

    principle_investigator = Column("principle_investigator", NVARCHAR(500))
    principle_investigator.create(t)
