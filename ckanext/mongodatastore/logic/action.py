import json
import logging

from ckan import logic

from ckanext.mongodatastore.controller.mongodb_controller import VersionedDataStoreController

log = logging.getLogger(__name__)


def issue_query_pid(context, data_dict):
    cntr = VersionedDataStoreController.get_instance()

    resource_id = data_dict.get('resource_id', '')

    default_projection = {}

    for field in cntr.resource_fields(resource_id)['schema']:
        default_projection[field['id']] = 1

    statement = data_dict.get('statement', {})
    q = data_dict.get('q', None)
    projection = data_dict.get('projection', default_projection)
    sort = data_dict.get('sort', None)
    distinct = data_dict.get('distinct', False)

    if sort:
        sort = sort.split(',')

    return cntr.issue_pid(resource_id, statement, projection, sort, distinct, q)


@logic.side_effect_free
def querystore_resolve(context, data_dict):
    cntr = VersionedDataStoreController.get_instance()

    pid = data_dict.get('pid')
    skip = data_dict.get('offset', 0)
    limit = data_dict.get('limit', 10)
    include_data = data_dict.get('include_data', None)

    if type(include_data) in [str, unicode]:
        include_data = bool(include_data)

    if skip:
        skip = int(skip)
    if limit:
        limit = int(limit)

    records_format = data_dict.get('records_format', 'objects')

    log.debug('querystore_resolve parameters {0}'.format([pid, skip, limit, records_format, include_data]))

    result = cntr.execute_stored_query(pid, offset=skip, limit=limit, preview=include_data)

    if 'records' in result.keys():
        result['records_preview'] = list(result.get('records'))
        result.pop('records')
    else:
        result['records_preview'] = None

    return result

@logic.side_effect_free
def nv_datastore_search(context, data_dict):
    cntr = VersionedDataStoreController.get_instance()

    resource_id = data_dict.get('resource_id')
    filters = data_dict.get('filters', '{}')
    #sort = data_dict.get('sort')

    offset = int(data_dict.get('offset', '0'))
    limit = int(data_dict.get('limit', '0'))

    filters = json.loads(filters)

    return cntr.query_nonversioned_store(resource_id, filters, offset, limit)