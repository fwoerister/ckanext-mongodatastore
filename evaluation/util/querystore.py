import json
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ckanext.mongodatastore.model import Query

logger = logging.getLogger(__name__)

config = json.load(open('../config.json', 'r'))['querystore']
engine = create_engine(config['db_url'], echo=False)
Session = sessionmaker(bind=engine)
session = Session()


def verify_handle_was_assigned(pid):
    query = session.query(Query).filter(Query.id == pid).first()
    assert query.handle_pid is not None
    assert query.result_set_hash is not None
    return query.handle_pid
