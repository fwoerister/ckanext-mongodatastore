from sqlalchemy import Column, BIGINT, TEXT, INT, ForeignKey, UniqueConstraint, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class Query(Base):
    def __init__(self):
        pass
    __tablename__ = 'QUERY'
    id = Column(BIGINT, primary_key=True)
    resource_id = Column(TEXT)
    handle_pid = Column(TEXT)
    timestamp = Column(TIMESTAMP)
    query = Column(TEXT)
    query_hash = Column(TEXT)
    result_set_hash = Column(TEXT)
    hash_algorithm = Column(TEXT)
    record_field_hash = Column(TEXT)
    record_fields = relationship("RecordField", lazy='joined', cascade='all, delete-orphan')
    metadata_fields = relationship("MetaDataField", lazy='joined', cascade='all, delete-orphan')


class MetaDataField(Base):
    def __init__(self):
        pass

    __tablename__ = 'META_DATA_FIELD'

    id = Column(BIGINT, primary_key=True)
    key = Column(TEXT)
    query_id = Column(BIGINT, ForeignKey('QUERY.id'))
    value = Column(TEXT)

    UniqueConstraint('key', 'query_id', name='key-query_id-unique-constraint')


class RecordField(Base):
    def __init__(self):
        pass

    __tablename__ = 'RECORD_FIELD'

    id = Column(BIGINT, primary_key=True)
    query_id = Column(BIGINT, ForeignKey('QUERY.id'))
    name = Column(TEXT)
    datatype = Column(TEXT)
    description = Column(TEXT)
    order = Column(INT)
