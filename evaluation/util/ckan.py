import json
import logging

import ckanapi
from ckanapi import NotFound

logger = logging.getLogger(__name__)

config = json.load(open('../config.json', 'r'))
client = ckanapi.RemoteCKAN(config['ckan']['url'], apikey=config['ckan']['apikey'])


def verify_if_evaluser_exists():
    username = config['ckan']['evaluser']
    result = client.action.user_show(id=username)
    assert (result is not None)


def verify_if_organization_exists(org_name):
    result = client.action.organization_show(id=org_name)
    assert (result is not None)


def ensure_package_does_not_exist(pkg_name):
    try:
        client.action.package_delete(id=pkg_name)
    except NotFound:
        logger.info("tried to delete non-existing package")


def verify_package_does_exist(pkg_name):
    try:
        client.action.package_show(id=pkg_name)
        found = True
    except NotFound as e:
        raise AssertionError(e)


def verify_package_contains_resource(pkg_name, expected_resource):
    package = client.action.package_show(id=pkg_name)

    assert (len(package['resources']) == 1)
    resource = package['resources'][0]

    assert (resource['name'] == expected_resource['name'])
    assert (resource['datastore_active'] == expected_resource['datastore_active'])
    return resource['id']


def verify_new_record_is_in_datastore(resource_id, new_record):
    result = client.action.datastore_search(resource_id=resource_id, filters={'id': new_record['id']})
    assert (len(result['records']) == 1)
    assert (result['records'][0] == new_record)


def verify_record_with_id_exists(resource_id, record_id):
    result = client.action.datastore_search(resource_id=resource_id, filters={'id': record_id})
    assert (len(result['records']) == 1)


def verify_record_with_id_does_not_exist(resource_id, record_id):
    result = client.action.datastore_search(resource_id=resource_id, filters={'id': record_id})
    assert (len(result['records']) == 0)
