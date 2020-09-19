import json
import logging

from ckan import logic

from ckanext.mongodatastore.controller.mongodb import VersionedDataStoreController

log = logging.getLogger(__name__)


def issue_query_pid(context, data_dict):
    cntr = VersionedDataStoreController.get_instance()

    resource_id = data_dict.get('resource_id', '')

    statement = data_dict.get('statement', {})
    q = data_dict.get('q', None)
    projection = data_dict.get('projection', [])
    sort = data_dict.get('sort', [])

    return cntr.issue_pid(resource_id, statement, projection, sort, q)


@logic.side_effect_free
def querystore_resolve(context, data_dict):
    cntr = VersionedDataStoreController.get_instance()

    internal_id = int(data_dict.get('id'))
    skip = data_dict.get('offset', 0)
    limit = data_dict.get('limit', 0)
    include_data = bool(data_dict.get('include_data', 'True'))

    if skip:
        skip = int(skip)
    if limit:
        limit = int(limit)

    records_format = data_dict.get('records_format', 'objects')

    log.debug('querystore_resolve parameters {0}'.format([internal_id, skip, limit, records_format, include_data]))

    result = cntr.execute_stored_query(internal_id, offset=skip, limit=limit, include_data=include_data)

    if 'records' in result.keys():
        result['records_preview'] = list(result.get('records'))
        result.pop('records')
    else:
        result['records_preview'] = None

    return result


@logic.side_effect_free
def nonversioned_query(context, data_dict):
    cntr = VersionedDataStoreController.get_instance()

    default_projection = {}

    resource_id = data_dict.get('resource_id')
    q = data_dict.get('q', None)
    projection = data_dict.get('projection', default_projection)
    sort = data_dict.get('sort', None)
    skip = data_dict.get('offset', 0)
    limit = data_dict.get('limit', 0)
    statement = json.loads(data_dict.get('filters', '{}'))

    log.debug('nv {}'.format(statement))
    log.debug('nv {}'.format(q))

    if sort:
        sort = sort.split(',')

    if skip:
        skip = int(skip)
    if limit:
        limit = int(limit)

    result = cntr.nv_query(resource_id, statement, q, projection, sort, skip, limit)

    if 'records' in result.keys():
        result['records_preview'] = list(result.get('records'))
        result.pop('records')
    else:
        result['records_preview'] = None

    return result
