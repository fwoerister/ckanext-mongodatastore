import logging

import evaluation.util.ckan as ckan
import evaluation.util.env as env
import evaluation.util.mongodb as mongodb

# DESCRIPTION
# Delete records of an existing datastore resource.

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

logging.info("Start execution of 'tc2_3'")

# PRE-REQUISIT
env.verify_containers_are_running()
ckan.verify_if_evaluser_exists()
ckan.verify_if_organization_exists('tu-wien')
ckan.verify_package_does_exist('rr-experiment')
resource_id = ckan.verify_package_contains_resource('rr-experiment',
                                                    {'name': 'RR_processed.csv', 'datastore_active': True})
ckan.verify_record_with_id_exists(resource_id, 2)

logging.info("pre-requisists are fullfilled")

# STEPS
ckan.client.action.datastore_delete(resource_id=resource_id, filters={'id': 2}, force=True)

# EXPECTED RESULTS
mongodb.verify_document_was_marked_as_deleted(resource_id, 2)
ckan.verify_record_with_id_does_not_exist(resource_id, 2)

logging.info("'tc2_3' successfully executed!")
