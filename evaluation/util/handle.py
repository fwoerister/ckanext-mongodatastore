import json
import logging

import requests

logger = logging.getLogger(__name__)

config = json.load(open('../config.json', 'r'))


def verify_handle_resolves_to_pid(handle_pid, pid):
    result = requests.get('{}/api/handles/{}'.format(config['handle']['base_url'], handle_pid))
    values = result.json()['values']

    for value in values:
        if value['type'] == 'URL':
            assert value['data']['value'] == '{}/querystore/view_query?id={}'.format(config['ckan']['site_url'], pid)
        elif value['type'] == 'API_URL':
            assert value['data']['value'] == '{}/api/3/action/querystore_resolve?pid={}' \
                .format(config['ckan']['site_url'], pid)
        elif value['type'] == 'STREAM_URL':
            assert value['data']['value'] == '{}/datadump/querystore_resolve/{}'.format(config['ckan']['site_url'], pid)
