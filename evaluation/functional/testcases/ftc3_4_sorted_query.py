import logging

import evaluation.util.ckan as ckan
import evaluation.util.env as env

# DESCRIPTION
# In this testcase a query is submitted to the datastore that retrieves all
# records sorted by a defined column.

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

logging.info("Start execution of 'tc3_4'")

# PRE-REQUISIT
env.verify_containers_are_running()
ckan.verify_if_evaluser_exists()
ckan.verify_if_organization_exists('tu-wien')
ckan.verify_package_does_exist('rr-experiment')
resource_id = ckan.verify_package_contains_resource('rr-experiment',
                                                    {'name': 'RR_processed.csv', 'datastore_active': True})

logging.info("pre-requisists are fullfilled")

# STEPS
result = ckan.client.action.datastore_search(resource_id=resource_id, filters={'Country': 'France'},
                                             sort="Dept asc", offset=0, limit=130)

# EXPECTED RESULTS
ckan.verify_resultset_record_count(result, 64)
print(result)
ckan.verify_resultset_record_hash(result, "5384adbee50eeaa00d7b48eae779534d")

logging.info("'tc3_4' successfully executed!")
