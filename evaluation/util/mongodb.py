import json
import logging

from pymongo import MongoClient

logger = logging.getLogger(__name__)

config = json.load(open('../config.json', 'r'))['mongodb']
mongo_client = MongoClient(config['url'])
db = mongo_client.get_database(config['database'])


def remove_datastore_entries_by_id(resource_id, id):
    db.get_collection(resource_id).delete_many({'id': id})


def verify_new_document_is_in_mongo_collection(resource_id, record):
    resource_collection = db.get_collection(resource_id)
    internal_record = resource_collection.find_one(record)
    assert (internal_record is not None)

    # A timestamp was added to the record that represents the point of time when the record was added to the resource
    assert (internal_record['_created'] is not None), "No creation timestamp was assigned to the new record"
    assert (internal_record['_latest']), "The new record was not marked as _latest"

    assert (resource_collection.find({'id': record['id'], '_latest': True}).count() == 1), \
        "More than one document with the same id was marked as _latest!"


def verify_document_was_marked_as_deleted(resource_id, id):
    resource_collection = db.get_collection(resource_id)
    count = resource_collection.find({'id': id, '_latest': True}).count()
    assert count == 0, "There is still a document in the datastore with id {} that is marked as _latest".format(id)
    records = list(resource_collection.find({'id': id}))

    for record in records:
        assert '_valid_to' in record.keys(), \
            'There is still a document with id {} and no _valid_to date in the collection'.format(id)