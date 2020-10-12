import json
import logging

from ckan import logic

from ckanext.mongodatastore.controller.mongodb import VersionedDataStoreController
from ckanext.mongodatastore.datastore_backend import MIN_LIMIT, MAX_LIMIT

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

    id = data_dict.get('id')

    skip = data_dict.get('offset', 0)
    limit = data_dict.get('limit', 0)
    include_data = data_dict.get('include_data', 'true').lower() == 'true'

    if skip:
        skip = int(skip)
    if limit:
        limit = int(limit)

    return cntr.execute_stored_query(id, offset=skip, limit=limit, include_data=include_data)


@logic.side_effect_free
def nv_query(context, data_dict):
    cntr = VersionedDataStoreController.get_instance()

    resource_id = data_dict.get('resource_id')
    q = data_dict.get('q', None)
    projection = data_dict.get('fields', [])
    sort = data_dict.get('sort', None)
    skip = int(data_dict.get('offset', 0))
    limit = int(data_dict.get('limit', 0))
    statement = json.loads(data_dict.get('filters', '{}'))

    if limit < MIN_LIMIT:
        limit = MIN_LIMIT

    if limit > MAX_LIMIT:
        limit = MAX_LIMIT

    if q:
        result = cntr.query_by_fulltext(resource_id, q, projection, sort, skip, limit, True, none_versioned=True)
    else:
        result = cntr.query_by_filters(resource_id, statement, projection, sort, skip, limit, True, False,
                                       none_versioned=True)

    result['offset'] = skip
    result['limit'] = limit

    return result
