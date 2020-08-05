import json
import logging

import requests

STREAM_TEMPLATE = '{}/datadump/querystore_resolve/{}'
API_TEMPLATE = '{}/api/3/action/querystore_resolve?pid={}'
URL_TEMPLATE = '{}/querystore/view_query?id={}'

logger = logging.getLogger(__name__)

config = json.load(open('../config.json', 'r'))


def verify_handle_resolves_to_pid(handle_pid, pid):
    result = requests.get('{}/api/handles/{}'.format(config['handle']['base_url'], handle_pid))
    values = result.json()['values']

    for value in values:
        if value['type'] == 'URL':
            assert value['data']['value'] == URL_TEMPLATE.format(config['ckan']['site_url'], pid)
        elif value['type'] == 'API_URL':
            assert value['data']['value'] == API_TEMPLATE.format(config['ckan']['site_url'], pid)
        elif value['type'] == 'STREAM_URL':
            assert value['data']['value'] == STREAM_TEMPLATE.format(config['ckan']['site_url'], pid)
