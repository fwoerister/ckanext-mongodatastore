import logging

import requests
import docker

logger = logging.getLogger(__name__)

c = docker.from_env()

# todo: add u'ckan'
EXPECTED_CONTAINERS = [u'mongodb', u'handle_server', u'mdb-shard03', u'redis', u'mdb-shard01', u'solr', u'db',
                       u'datapusher', u'gitlab', u'mdb-shard02', u'mdb-config']


def verify_containers_are_running(expected_containers=EXPECTED_CONTAINERS):
    running_containers = [cont.name for cont in c.containers.list()]
    for container in expected_containers:
        assert container in running_containers, "Container '{}' is not running!".format(container)


def is_available(url):
    result = requests.get(url)
    assert result.status_code == 200, "The response status of {} was {}".format(url, result.status_code)
