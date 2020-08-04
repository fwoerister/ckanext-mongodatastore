import json
import logging
import requests

logger = logging.getLogger(__name__)

config = json.load(open('../config.json', 'r'))['solr']


def index_exists(resource_name):
    response = requests.get('{}/{}/select?q=name:{}&wt=json'.format(config['url'], config['core'], resource_name))
    assert (response.status_code == 200)

    json_response = json.loads(response.text)
    assert (json_response['response']['numFound'] == 1)
