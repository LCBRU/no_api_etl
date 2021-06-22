from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    NVARCHAR,
    BOOLEAN,
    DateTime,
)

meta = MetaData()


def upgrade(migrate_engine):
    meta.bind = migrate_engine

    t = Table(
        "edge_site_study",
        meta,
        Column("id", Integer, primary_key=True),
        Column("project_short_title", NVARCHAR(500)),
        Column("project_id", Integer, index=True, nullable=False),
        Column("principle_investigator", NVARCHAR(500)),
        Column("project_type", NVARCHAR(500)),
        Column("research_theme", NVARCHAR(500)),
        Column("start_date", DateTime),
        Column("project_site_date_open_to_recruitment", DateTime),
        Column("project_site_start_date_nhs_permission", DateTime),
        Column("end_date", DateTime),
        Column("project_site_closed_date", DateTime),
        Column("project_site_planned_closing_date", DateTime),
        Column("recruitment_end_date", DateTime),
        Column("project_site_actual_recruitment_end_date", DateTime),
        Column("project_site_planned_recruitment_end_date", DateTime),
        Column("recruited_total", Integer),
        Column("recruited_org", Integer),
        Column("project_status", NVARCHAR(500)),
        Column("project_site_target_participants", Integer),
    )
    t.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    t = Table("edge_site_study", meta, autoload=True)
    t.drop()
