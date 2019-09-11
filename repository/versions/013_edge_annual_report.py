from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    NVARCHAR,
    BOOLEAN,
    ForeignKey,
    DateTime,
)

meta = MetaData()


def upgrade(migrate_engine):
    meta.bind = migrate_engine

    t = Table(
        "edge_annual_report",
        meta,
        Column("id", Integer, primary_key=True),
        Column("project_id", Integer, index=True, nullable=False),
        Column("full_title", NVARCHAR(500)),
        Column("short_title", NVARCHAR(100)),
        Column("mrec_number", NVARCHAR(100)),
        Column("principle_investigator", NVARCHAR(100)),
        Column("pi_orcid", NVARCHAR(100)),
        Column("start_date", DateTime),
        Column("end_date", DateTime),
        Column("status", NVARCHAR(100)),
        Column("research_theme", NVARCHAR(100)),
        Column("ukcrc_health_category", NVARCHAR(100)),
        Column("main_speciality", NVARCHAR(100)),
        Column("disease_area", NVARCHAR(100)),
        Column("project_type", NVARCHAR(100)),
        Column("primary_intervention_or_area", NVARCHAR(100)),
        Column("randomisation", NVARCHAR(100)),
        Column("recruited_total", Integer),
        Column("funders", NVARCHAR(500)),
        Column("funding_category", NVARCHAR(100)),
        Column("total_external_funding_awarded", NVARCHAR(100)),
        Column("is_uhl_lead_centre", BOOLEAN),
        Column("lead_centrename_if_not_uhl", NVARCHAR(500)),
        Column("multicentre", BOOLEAN),
        Column("first_in_human_centre", BOOLEAN),
        Column("link_to_nhir_translational_research_collaboration", BOOLEAN),
    )
    t.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    t = Table("edge_annual_report", meta, autoload=True)
    t.drop()
