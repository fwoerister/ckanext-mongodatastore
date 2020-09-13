import logging
from datetime import datetime

import pymongo
import pytz
from ckan.common import config
from ckan.plugins import toolkit
from pymongo import MongoClient
from pymongo.collation import Collation
from pymongo.errors import BulkWriteError

from ckanext.mongodatastore.controller.querystore import QueryStoreController
from ckanext.mongodatastore.exceptions import MongoDbControllerException, QueryNotFoundException
from ckanext.mongodatastore.query_preprocessor import transform_query_to_statement, transform_filter, create_projection, \
    transform_sort
from ckanext.mongodatastore.util import normalize_json, HASH_ALGORITHM, calculate_hash

log = logging.getLogger(__name__)

type_conversion_dict = {
    'string': str,
    'str': str,
    'text': str,
    'char': str,
    'integer': int,
    'int': int,
    'float': float,
    'number': float,
    'numeric': float,
    'bigint': long
}


def calculate_resultset_hash_job(pid):
    client = MongoClient(config.get(u'ckan.datastore.write_url'))
    querystore = QueryStoreController(config.get(u'ckanext.mongodatastore.querystore_url'))

    q, metadata = querystore.retrieve_query(pid)
    c = client.get_database(config.get(u'ckanext.mongodatastore.database_name')).get_collection(q.resource_id)
    stored_query = q.query

    log.info("fetch {0}".format(stored_query['filter']))

    stored_query['filter'].update({
        '_valid_to': {'$gt': q.timestamp},
        '_created': {'$lte': q.timestamp}
    })

    result = c.find(filter=stored_query['filter'], projection=stored_query['projection'], sort=stored_query['sort'])

    _hash = calculate_hash(result)
    querystore.update_hash(pid, _hash)


class VersionedDataStoreController:
    def __init__(self):
        pass

    instance = None

    class __VersionedDataStoreController:
        def __init__(self, client, database_name, sharding_enabled, querystore, rows_max, queue_name, ckan_site_url):
            self.client = client
            self.datastore = self.client.get_database(database_name)
            self.sharding_enabled = sharding_enabled
            self.querystore = querystore
            self.rows_max = rows_max
            self.queue_name = queue_name
            self.ckan_site_url = ckan_site_url

        @staticmethod
        def _execute_query(col, distinct, limit, offset, projected_schema, projection, sort, statement):
            if distinct:
                return [{projected_schema[0]['id']: val} for val in
                        col.distinct(projected_schema[0]['id'])]
            else:
                curs = col.find(filter=statement, projection=projection, skip=offset,
                                limit=limit)

                log.debug('query')
                log.debug(statement)
                log.debug(projection)
                log.debug(offset)
                log.debug(limit)

                if sort:
                    return curs.sort(sort)
                return curs

        @staticmethod
        def _prepare_projection(projection):
            if projection and 1 in projection.values():
                projection.update({'_id': 0})
            else:
                projection = {'_id': 0, '_created': 0, '_valid_to': 0, '_latest': 0, '_hash': 0}

            return normalize_json(projection)

        @staticmethod
        def __apply_override_type(records, fields):
            field_type = dict()

            for field in fields.find():
                if 'info' in field and 'type_override' in field['info']:
                    if field['info']['type_override'] in type_conversion_dict:
                        field_type[field['id']] = type_conversion_dict[field['info']['type_override']]
                else:
                    field_type[field['id']] = type_conversion_dict[field['type']]

            for record in records:
                for field in record.keys():
                    if record[field]:
                        if field_type[field] in [float, int] and (record[field] == '' or str(record[field]).isspace()):
                            record[field] = None
                        else:
                            try:
                                record[field] = field_type[field](record[field])
                            except ValueError as e:
                                print(e)

        def __get_collections(self, resource_id):
            col = self.datastore.get_collection(resource_id)
            meta = self.datastore.get_collection('{0}_meta'.format(resource_id))
            fields = self.datastore.get_collection('{0}_fields'.format(resource_id))
            return col, meta, fields

        def __update_required(self, resource_id, new_record, id_key):
            col, meta, _ = self.__get_collections(resource_id)

            result = col.find_one(
                {'_latest': True, '_hash': new_record['_hash'], id_key: new_record[id_key]}, {'_hash': 1})
            return result is None

        def get_all_ids(self):
            return [name for name in self.datastore.list_collection_names() if
                    not (name.endswith('_meta') or name.endswith('_fields'))]

        def resource_exists(self, resource_id):
            return resource_id in self.datastore.list_collection_names()

        def create_resource(self, resource_id, primary_key):
            if resource_id not in self.datastore.list_collection_names():
                self.datastore.create_collection(resource_id)
                self.datastore.create_collection('{0}_meta'.format(resource_id))

            if self.sharding_enabled:
                self.client.admin.command('shardCollection', 'CKAN_Datastore.{0}'.format(resource_id),
                                          key={'_id': 'hashed'})

            self.datastore.get_collection('{0}_meta'.format(resource_id)).insert_one(
                {'record_id': primary_key, 'active': True})

            col = self.datastore.get_collection(resource_id)

            col.create_index(
                [('_created', pymongo.ASCENDING), ('_valid_to', pymongo.DESCENDING), ('_id', pymongo.ASCENDING)],
                name='_created_valid_to_index')

            col.create_index([('_latest', pymongo.DESCENDING), ('_id', pymongo.ASCENDING)],
                             name='_valid_to_pk_index')

        def delete_resource(self, resource_id, filters={}):
            col = self.datastore.get_collection(resource_id)
            filters.update({'_latest': True})
            col.update_many(filters, {'$currentDate': {'_valid_to': True}, '$set': {'_latest': False}})

        def update_schema(self, resource_id, field_definitions, indexes, primary_key):
            collection, _, fields = self.__get_collections(resource_id)
            fields.delete_many({})
            fields.insert_many(field_definitions)

            type_dict = {}
            text_fields = []

            for field in field_definitions:
                if field['type'] == 'text':
                    text_fields.append(field['id'])
                type_dict[field['id']] = field['type']

            if indexes:
                for index in indexes:
                    if index != primary_key and index not in text_fields:
                        collection.create_index([(index, pymongo.ASCENDING)], name='{0}_index'.format(index))
                    else:
                        collection.create_index([(field['id'], 1)], collation=Collation(locale='en'))
            for field in field_definitions:
                field.pop('_id')

        def insert(self, resource_id, records, dry_run=False):
            col, meta, fields = self.__get_collections(resource_id)
            record_id_key = meta.find_one()['record_id']

            self.__apply_override_type(records, fields)

            records_without_id = [record for record in records if record_id_key not in record.keys()]

            if len(records_without_id) > 0:
                raise MongoDbControllerException('For a datastore upsert, an id '
                                                 'value has to be set for every record. '
                                                 'In this collection the id attribute is "{0}"'.format(record_id_key))

            if not dry_run:
                for record in records:
                    record['_hash'] = calculate_hash(record)
                    record['_valid_to'] = datetime.max

                try:
                    col.insert_many(records)
                    col.update_many({'_created': {'$exists': False}},
                                    {'$currentDate': {'_created': True},
                                     '$set': {'_latest': True}})
                except BulkWriteError as bwe:
                    log.error(bwe.details)

        def upsert(self, resource_id, records, dry_run=False):
            col, meta, fields = self.__get_collections(resource_id)
            record_id_key = meta.find_one()['record_id']

            self.__apply_override_type(records, fields)

            records_without_id = [record for record in records if record_id_key not in record.keys()]

            if len(records_without_id) > 0:
                raise MongoDbControllerException('For a datastore upsert, an id '
                                                 'value has to be set for every record. '
                                                 'In this collection the id attribute is "{0}"'.format(record_id_key))

            required_updates = []
            for record in records:
                if not dry_run:
                    record['_hash'] = calculate_hash(record)
                    if self.__update_required(resource_id, record, record_id_key):
                        record['_latest'] = True
                        record['_valid_to'] = datetime.max
                        required_updates.append(record)

            col.update_many({'id': {'$in': [record[record_id_key] for record in required_updates]},
                             '_latest': True},
                            {'$currentDate': {'_valid_to': True},
                             '$set': {'_latest': False}})
            col.insert_many(required_updates)
            col.update_many({'_created': {'$exists': False}},
                            {'$currentDate': {'_created': True},
                             '$set': {'_latest': True}})

        def issue_pid(self, resource_id, statement, projection, sort, q):
            now = datetime.now(pytz.UTC)

            col, meta, fields = self.__get_collections(resource_id)
            result = dict()
            schema = fields.find()

            if q:
                statement = transform_query_to_statement(q, schema)
            else:
                statement = transform_filter(statement, schema)

            statement = normalize_json(statement)

            projection = create_projection(fields.find(), projection)

            if sort:
                sort = transform_sort(sort) + [('_id', 1)]
            else:
                sort = [('_id', 1)]

            projected_schema = [field for field in fields.find() if field[u'id'] in projection.keys()]

            query, meta_data = self.querystore.store_query(resource_id,
                                                           {'filter': statement,
                                                            'projection': projection,
                                                            'sort': sort},
                                                           str(now),
                                                           None, HASH_ALGORITHM().name,
                                                           projected_schema)

            toolkit.enqueue_job(calculate_resultset_hash_job, [query.id], queue=self.queue_name)

            result['metadata'] = meta_data
            result['query'] = query
            fields = []
            for field in query.record_fields:
                fields.append({
                    'id': field.name,
                    'type': field.datatype,
                    'info': {
                        'description': field.description
                    }
                })
            result['fields'] = fields
            return query.id

        def execute_stored_query(self, pid, offset, limit, preview=False):
            log.debug("execute_stored_query")
            q, metadata = self.querystore.retrieve_query(pid)

            if q:
                log.debug("query found")
                col, meta, _ = self.__get_collections(q.resource_id)
                stored_query = {
                    '_valid_to': {'$gt': q.timestamp},
                    '_created': {'$lte': q.timestamp}
                }
                result = dict()

                result['pid'] = pid

                if preview:
                    stored_query.update(q.query['filter'])
                    result['records'] = col.find(filter=stored_query,
                                                 projection=q.query.get('projection'),
                                                 sort=q.query.get('sort'),
                                                 skip=offset,
                                                 limit=limit,
                                                 hint='_created_valid_to_index')

                query = {
                    'id': q.id,
                    'resource_id': q.resource_id,
                    'query': q.query,
                    'query_hash': q.query_hash,
                    'hash_algorithm': q.hash_algorithm,
                    'result_set_hash': q.result_set_hash,
                    'timestamp': str(q.timestamp),
                    'handle_pid': q.handle_pid
                }
                result['query'] = query

                fields = []
                field_names = []
                for field in q.record_fields:
                    field_names.append(field.name)
                    fields.append({
                        'id': field.name,
                        'type': field.datatype,
                        'info': {
                            'description': field.description
                        }
                    })

                result['fields'] = fields
                result['meta'] = metadata

                return result
            else:
                log.debug("query not found")
                raise QueryNotFoundException('No query with PID {0} found'.format(pid))

        def query_current_state(self, resource_id, statement, projection, sort, offset, limit, distinct, include_total,
                                projected_schema):
            col, _, _ = self.__get_collections(resource_id)
            result = dict()

            statement['_latest'] = True

            if sort:
                sort = sort + [('_id', 1)]
            else:
                sort = [('_id', 1)]

            if include_total:
                result['total'] = col.count_documents(statement)

            projection = self._prepare_projection(projection)

            res = self._execute_query(col, distinct, limit, offset, projected_schema, projection, sort,
                                      statement)

            result['records'] = list(res)

            if type(res) != list:
                res.close()

            return result

        def resource_fields(self, resource_id):
            col, meta, fields = self.__get_collections(resource_id)

            meta_entry = meta.find_one({}, {"_id": 0})
            schema = fields.find({}, {'_id': 0})

            return {'meta': meta_entry, 'schema': list(schema)}

        def nv_query(self, resource_id, statement, q, projection, sort, skip, limit):
            col, _, fields = self.__get_collections(resource_id)
            result = dict()

            schema = fields.find({}, {'_id': 0})

            if q:
                statement = transform_query_to_statement(q, schema)
            else:
                statement = transform_filter(statement, schema)

            if sort:
                sort = sort + [('_id', 1)]
            else:
                sort = [('_id', 1)]

            result['total'] = col.count_documents(statement)

            projection = self._prepare_projection(projection)

            res = self._execute_query(col, False, limit, skip, None, projection, sort,
                                      statement)

            result['records'] = list(res)

            if type(res) != list:
                res.close()

            return result

    @classmethod
    def get_instance(cls):
        if VersionedDataStoreController.instance is None:

            mongodb_url = config.get(u'ckanext.mongodatastore.mongodb_url')
            querystore_url = config.get(u'ckanext.mongodatastore.querystore_url')
            sharding_enabled = bool(config.get(u'ckanext.mongodatastore.sharding_enabled'))
            database_name = config.get(u'ckanext.mongodatastore.database_name')
            rows_max = config.get(u'ckan.mongodatastore.max_result_size', 500)
            ckan_site_url = config.get(u'ckan.site_url')

            queue_name = config.get(u'ckan.mongodatastore.queue_name', 'hash_queue')

            client = MongoClient(mongodb_url)
            querystore = QueryStoreController(querystore_url)

            if sharding_enabled:
                client.admin.command('enableSharding', database_name)

            cls.instance = VersionedDataStoreController.__VersionedDataStoreController(client,
                                                                                       database_name,
                                                                                       sharding_enabled,
                                                                                       querystore,
                                                                                       rows_max,
                                                                                       queue_name,
                                                                                       ckan_site_url)

        return VersionedDataStoreController.instance

    @classmethod
    def reload_config(cls, cfg):
        cls.instance.client.close()
        if cls.instance.querystore.session:
            cls.instance.querystore.session.close()

        cls.instance = None
        cls.get_instance()
