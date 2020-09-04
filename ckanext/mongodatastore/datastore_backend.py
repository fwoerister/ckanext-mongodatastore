import logging

from ckan.lib.base import abort
from ckan.logic import get_action
from ckanext.datastore.backend import DatastoreBackend

from ckanext.mongodatastore.controller.mongodb_controller import VersionedDataStoreController
from ckanext.mongodatastore.query_preprocessor import transform_query_to_statement, transform_filter, transform_sort, \
    create_projection

log = logging.getLogger(__name__)

MIN_LIMIT = 1
MAX_LIMIT = 500


def raise_exeption(ex):
    raise ex


def log_parameter_not_used_warning(param_list):
    for param in param_list:
        if param[1]:
            log.debug('The parameter {0} is set, but has no effect in this DataStore backend implementation!'
                      .format(param[0]))


class MongoDataStoreBackend(DatastoreBackend):
    def __init__(self):
        self.mongo_cntr = VersionedDataStoreController.get_instance()
        self.enable_sql_search = True

    def configure(self, cfg):
        VersionedDataStoreController.reload_config(cfg)
        return cfg

    def create(self, context, data_dict):
        resource_id = data_dict.get('resource_id', None)
        records = data_dict.get('records', None)
        force = data_dict.get('force', False)
        resource = data_dict.get('resource', None)
        aliases = data_dict.get('aliases', None)
        fields = data_dict.get('fields', None)
        primary_key = data_dict.get('primary_key', 'id')
        indexes = data_dict.get('indexes', None)
        triggers = data_dict.get('triggers', None)
        calculate_record_count = data_dict.get('calculate_record_count', False)

        if 'records' in data_dict:
            data_dict['records'] = None

        log_parameter_not_used_warning(
            [('force', force), ('resource', resource), ('aliases', aliases),
             ('triggers', triggers), ('calculate_record_count', calculate_record_count)])

        if indexes:
            indexes = indexes.split(',')

        self.mongo_cntr.update_schema(resource_id, fields, indexes, primary_key)
        self.mongo_cntr.create_resource(resource_id, primary_key)

        if records:
            self.mongo_cntr.upsert(resource_id, records)

        return data_dict

    def upsert(self, context, data_dict):
        resource_id = data_dict.get(u'resource_id')
        force = data_dict.get(u'force', False)
        records = data_dict.get(u'records')
        method = data_dict.get(u'method', 'upsert')
        calculate_record_count = data_dict.get(u'calculate_record_count', False)
        dry_run = data_dict.get(u'dry_run', False)

        log_parameter_not_used_warning(
            [('force', force), ('calculate_record_count', calculate_record_count)])

        operations = {
            'insert': self.mongo_cntr.insert,
            'upsert': self.mongo_cntr.upsert,
            'update': lambda a, b, c: raise_exeption(NotImplementedError())
        }

        upsert_operation = operations[method]
        upsert_operation(resource_id, records, dry_run)

        data_dict['records'] = []
        return data_dict

    def delete(self, context, data_dict):
        resource_id = data_dict.get(u'resource_id')
        force = data_dict.get(u'force', False)
        filters = data_dict.get('filters', {})
        calculate_record_count = data_dict.get(u'calculate_record_count', False)

        log_parameter_not_used_warning(
            [('force', force), ('calculate_record_count', calculate_record_count)])

        self.mongo_cntr.delete_resource(resource_id, filters)

        return data_dict

    def search(self, context, data_dict):
        resource_id = data_dict.get(u'resource_id')
        filters = data_dict.get(u'filters', {})
        query = data_dict.get(u'q', None)
        distinct = data_dict.get(u'distinct', False)
        plain = data_dict.get(u'plain', True)
        language = data_dict.get(u'language', u'english')
        limit = data_dict.get(u'limit', 1)
        offset = data_dict.get(u'offset', 0)
        fields = data_dict.get(u'fields', [])
        sort = data_dict.get(u'sort', None)
        include_total = data_dict.get(u'include_total', True)
        total_estimation_threshold = data_dict.get(u'total_estimation_threshold', None)
        records_format = data_dict.get(u'records_format', u'objects')

        if limit < MIN_LIMIT:
            limit = MIN_LIMIT

        if limit > MAX_LIMIT:
            limit = MAX_LIMIT

        log_parameter_not_used_warning([(u'plain', plain), (u'language', language),
                                        (u'total_estimation_threshold', total_estimation_threshold)])

        if records_format in [u'tsv', u'lists']:
            abort(501, u"The current version of MongoDatastore only supports CSV exports!")

        schema = self.resource_fields(data_dict[u'resource_id'])[u'schema']

        if type(fields) is not list:
            fields = fields.split(',')
        projection = create_projection(schema, fields)

        projected_schema = [field for field in schema if field[u'id'] in projection.keys()]

        if query:
            statement = transform_query_to_statement(query, schema)
        else:
            statement = transform_filter(filters, schema)

        if sort:
            sort = transform_sort(sort.split(','))

        result = self.mongo_cntr.query_current_state(resource_id, statement, projection, sort, offset, limit, distinct,
                                                     include_total, projected_schema)

        result['offset'] = offset
        result['limit'] = limit
        result['fields'] = schema

        return result

    def search_sql(self, context, data_dict):
        raise NotImplementedError()

    def resource_exists(self, id):
        exists = self.mongo_cntr.resource_exists(id)
        res_metadata = get_action('resource_show')(None, {'id': id})

        return exists and res_metadata['datastore_active']

    def resource_fields(self, resource_id):
        return self.mongo_cntr.resource_fields(resource_id)

    def resource_info(self, resource_id):
        return self.resource_fields(resource_id)

    def resource_id_from_alias(self, alias):
        if self.resource_exists(alias):
            return True, alias
        return False, alias

    def get_all_ids(self):
        return self.mongo_cntr.get_all_ids()

    def create_function(self, *args, **kwargs):
        raise NotImplementedError()

    def drop_function(self, *args, **kwargs):
        raise NotImplementedError()
