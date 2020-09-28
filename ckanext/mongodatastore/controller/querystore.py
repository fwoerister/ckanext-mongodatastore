import json
import logging

from ckan.common import config
from ckan.logic import get_action
from easyhandle.client import BasicAuthHandleClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ckanext.mongodatastore.exceptions import QueryNotFoundException
from ckanext.mongodatastore.model import Query, RecordField, MetaDataField
from ckanext.mongodatastore.util import calculate_hash

LANDING_PAGE_URL_TEMPLATE = '{}/storedquery/landingpage?id={}'
API_URL_TEMPLATE = '{}/api/3/action/querystore_resolve?id={}'
STREAM_URL_TEMPLATE = '{}/storedquery/{}/dump'

CKAN_SITE_URL = config.get(u'ckan.site_url')

log = logging.getLogger(__name__)


class QueryStoreController:
    def __init__(self, querystore_url):
        self.engine = create_engine(querystore_url, echo=False)

        with open('/etc/ckan/cred.json') as config_file:
            config = json.loads(config_file.read())
        self.handle_client = BasicAuthHandleClient.load_from_config(config)

        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def _create_handle_entry(self, internal_id):
        landing_page = LANDING_PAGE_URL_TEMPLATE.format(CKAN_SITE_URL, str(internal_id))
        api_url = API_URL_TEMPLATE.format(CKAN_SITE_URL, str(internal_id))
        stream_url = STREAM_URL_TEMPLATE.format(CKAN_SITE_URL, str(internal_id))

        response = self.handle_client.put_handle_for_urls({
            'URL': landing_page,
            'API_URL': api_url,
            'STREAM_URL': stream_url
        })

        return response.json().get('handle')

    def store_query(self, resource_id, query, timestamp, result_hash,
                    hash_algorithm, fields_metadata):

        query_hash = calculate_hash(query)
        if fields_metadata:
            record_field_hash = calculate_hash(fields_metadata)
        else:
            record_field_hash = None

        q = Query()
        q.resource_id = resource_id
        q.query = query
        q.query_hash = query_hash
        q.result_set_hash = result_hash
        q.timestamp = timestamp
        q.hash_algorithm = hash_algorithm
        q.record_field_hash = record_field_hash

        resource_metadata = get_action('resource_show')(None, {'id': resource_id})
        package_metadata = get_action('package_show')(None, {'id': resource_metadata['package_id']})

        self.session.add(q)
        self.session.commit()

        metadata = {
            'citation_title': package_metadata['title'],
            'citation_author': package_metadata['author'],
            'citation_maintainer': package_metadata['maintainer'],
            'citation_filename': resource_metadata['name']
        }

        for entry in package_metadata['extras']:
            metadata['citation_' + entry['key']] = entry['value']

        for key in metadata:
            meta_entry = MetaDataField()
            meta_entry.key = key
            meta_entry.value = metadata[key]
            meta_entry.query_id = q.id
            self.session.add(meta_entry)
        self.session.commit()

        if fields_metadata:
            order = 0
            for field in fields_metadata:
                r = RecordField()
                r.name = field['id']
                r.datatype = field['type']
                r.order = order
                order += 1

                if 'info' in field.keys() and 'label' in field['info'].keys():
                    r.description = field['info']['label']

                if 'info' in field.keys() and 'label' in field['info'].keys():
                    if r.description:
                        r.description += ' - ' + field['info']['notes']
                    else:
                        r.description = field['info']['notes']

                r.query_id = q.id
                self.session.add(r)

        self.session.commit()
        self.session.flush()
        return q, metadata

    def update_hash(self, internal_id, result_hash):
        log.info('try to update query %s with hash %s', internal_id, result_hash)

        staged_query = self.session.query(Query).filter(Query.id == internal_id).first()
        q = self.session.query(Query).filter(Query.query_hash == staged_query.query_hash,
                                             Query.result_set_hash == result_hash,
                                             Query.result_set_hash is not None,
                                             Query.handle_pid is not None,
                                             Query.record_field_hash == staged_query.record_field_hash).first()

        if q:
            meta_data = {}

            staged_query.handle_pid = q.handle_pid
            staged_query.result_set_hash = result_hash
            self.session.merge(staged_query)
            self.session.commit()
            self.session.flush()
            return q, meta_data
        else:
            staged_query.handle_pid = self._create_handle_entry(internal_id)
            staged_query.result_set_hash = result_hash
            self.session.merge(staged_query)
            self.session.commit()
            metadata = {'citation_handle_pid': staged_query.handle_pid}

            for key in metadata:
                meta_entry = MetaDataField()
                meta_entry.key = key
                meta_entry.value = metadata[key]
                meta_entry.query_id = staged_query.id
                self.session.add(meta_entry)
            self.session.commit()
            self.session.flush()
            return staged_query, metadata

    def retrieve_query_by_internal_id(self, internal_id):
        result = self.session.query(Query).filter(Query.id == internal_id).first()

        if not result:
            raise QueryNotFoundException()

        meta_data = {}
        for meta_field in self.session.query(MetaDataField).filter(MetaDataField.query_id == result.id):
            meta_data[meta_field.key] = meta_field.value

        return result, meta_data

    def retrieve_query_by_pid(self, pid):
        result = self.session.query(Query).filter(Query.handle_pid.like(str(pid))).first()

        if not result:
            raise QueryNotFoundException()

        meta_data = {}
        for meta_field in self.session.query(MetaDataField).filter(MetaDataField.query_id == result.id):
            meta_data[meta_field.key] = meta_field.value

        return result, meta_data

    def get_cursor_on_ids(self):
        return self.session.query(Query.id).all()

    def purge_query_store(self):
        self.session.query(Query).delete()
        self.session.commit()
        self.session.flush()
