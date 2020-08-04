import logging

import evaluation.util.ckan as ckan
import evaluation.util.env as env

# DESCRIPTION
# In this testcase a query is submitted to the datastore that retrieves all records where a specific
# field exactly matches the provided parameter. For the resulting dataset a PID is issued.

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

logging.info("Start execution of 'tc3_1'")

# PRE-REQUISIT
env.verify_containers_are_running()
ckan.verify_if_evaluser_exists()
ckan.verify_if_organization_exists('tu-wien')
ckan.verify_package_does_exist('rr-experiment')
resource_id = ckan.verify_package_contains_resource('rr-experiment',
                                                    {'name': 'RR_processed.csv', 'datastore_active': True})

logging.info("pre-requisists are fullfilled")

# STEPS
result = ckan.client.action.datastore_search(resource_id=resource_id, filters={'Country': 'Austria'}, offset=0, limit=100)

# EXPECTED RESULTS
ckan.verify_resultset_record_count(result, 64)
ckan.verify_resultset_record_hash(result, "3a7777e3a648d1fd1731ab1f25bae88d")

logging.info("'tc3_1' successfully executed!")
