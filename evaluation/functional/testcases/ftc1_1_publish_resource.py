import logging
from time import sleep

import evaluation.util.ckan as ckan
import evaluation.util.env as env
import evaluation.util.solr as solr

# DESCRIPTION
# The objective of this test case is to evaluate the publication feature
# of the proposed system by uploading the dataset of the Reinhard & Rogoff
# experiment to it. This will also cover the publication of the related metadata
# of the published dataset.

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

logging.info("Start execution of 'tc1_1'")

# PRE-REQUISIT
env.verify_containers_are_running()
ckan.verify_if_evaluser_exists()
ckan.verify_if_organization_exists('tu-wien')
ckan.ensure_package_does_not_exist('rr-experiment')

logging.info("pre-requisists are fullfilled")

# STEPS
package = ckan.client.action.package_create(name='rr-experiment', title='Reinhard&Rogoff Experiment Data',
                                            private=False,
                                            owner_org='dc13c7c9-c3c9-42ac-8200-8fe007c049a1',
                                            author='Carmen Reinhart; Kenneth Rogoff',
                                            maintainer='', license='other-open',
                                            extras=[{'key': 'year', 'value': '2010'}])

resource = ckan.client.action.resource_create(package_id=package['id'],
                                              name='RR_processed.csv',
                                              upload=open('dataset/RR_processed.csv', 'r'))

logging.info("wait 10 seconds for datapusher...")
sleep(10)

# EXPECTED RESULTS
# *) package was indexed
solr.index_exists('rr-experiment')

# *) the package is stored in ckan
package = ckan.client.action.package_show(id='rr-experiment')
assert (package is not None)

# *) the metadata is stored in ckan
assert (package['author'] == 'Carmen Reinhart; Kenneth Rogoff')
assert (package['extras'] == [{'key': 'year', 'value': '2010'}])
logging.info("'tc1_1' successfully executed!")
