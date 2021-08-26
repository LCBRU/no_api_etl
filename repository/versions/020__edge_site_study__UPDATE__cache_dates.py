from sqlalchemy import (
    MetaData,
    Table,
    Column,
    NVARCHAR,
    Date,
    Integer,
    Index,
)
meta = MetaData()


def upgrade(migrate_engine):
    meta.bind = migrate_engine

    t = Table("edge_site_study", meta, autoload=True)

    effective_recruitment_start_date = Column("effective_recruitment_start_date", Date)
    effective_recruitment_start_date.create(t)

    ix_effective_recruitment_start_date = Index('ix_effective_recruitment_start_date', effective_recruitment_start_date)
    ix_effective_recruitment_start_date.create(migrate_engine)

    effective_recruitment_end_date = Column("effective_recruitment_end_date", Date)
    effective_recruitment_end_date.create(t)

    ix_effective_recruitment_end_date = Index('ix_effective_recruitment_end_date', effective_recruitment_end_date)
    ix_effective_recruitment_end_date.create(migrate_engine)

    target_end_date = Column("target_end_date", Date)
    target_end_date.create(t)

    target_end_date_description = Column("target_end_date_description", NVARCHAR(100))
    target_end_date_description.create(t)

    target_requirement_by = Column("target_requirement_by", Integer)
    target_requirement_by.create(t)

    current_target_recruited_percent = Column("current_target_recruited_percent", Integer)
    current_target_recruited_percent.create(t)

    rag_rating = Column("rag_rating", NVARCHAR(100))
    rag_rating.create(t)

    ix_rag_rating = Index('ix_rag_rating', rag_rating)
    ix_rag_rating.create(migrate_engine)


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    t = Table("edge_site_study", meta, autoload=True)

    t.c.effective_recruitment_start_date.drop()
    t.c.effective_recruitment_end_date.drop()
    t.c.target_end_date.drop()
    t.c.target_end_date_description.drop()
    t.c.target_requirement_by.drop()
    t.c.current_target_recruited_percent.drop()
    t.c.rag_rating.drop()
