from sqlalchemy import Table, MetaData, Column, TEXT

def upgrade(migrate_engine):
    meta = MetaData(bind=migrate_engine)
    t = Table('etl_task_message', meta, autoload=True)

    attachment = Column('attachment', TEXT)
    attachment.create(t)

def downgrade(migrate_engine):
    meta = MetaData(bind=migrate_engine)
    t = Table('etl_task_message', meta, autoload=True)
    t.c.attachment.drop()
