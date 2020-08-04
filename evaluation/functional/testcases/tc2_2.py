import logging

import evaluation.util.ckan as ckan
import evaluation.util.env as env
import evaluation.util.mongodb as mongodb

# DESCRIPTION
# Update exsting records of an datastore resource.


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

logging.info("Start execution of 'tc2_2'")

# PRE-REQUISIT
#   *) All required services (specified in the provided docker-compose file) are up and running.
env.verify_containers_are_running()
ckan.verify_if_evaluser_exists()
ckan.verify_if_organization_exists('tu-wien')
ckan.verify_package_does_exist('rr-experiment')
resource_id = ckan.verify_package_contains_resource('rr-experiment',
                                                    {'name': 'RR_processed.csv', 'datastore_active': True})
ckan.verify_record_with_id_exists(resource_id, 1)

logging.info("pre-requisists are fullfilled")

# STEPS
#  *) update existing record
new_record = {'id': 1, 'Country': 1, 'Year': 2000, 'Debt': 'NA', 'RGDP': 'NA', 'GDP': 'NA',
              'dRGDP': 'NA', 'GDPI': 'NA', 'GDP1': 'NA', 'GDP2': 'NA', 'RGDP1': 'NA',
              'RGDP2': 'NA', 'GDPI1': 'NA', 'GDPI2': 'NA', 'Infl': 'NA', 'Debt1': 'NA', 'Debt2': 'NA',
              'Debtalt': 'NA', 'GDP2alt': 'NA', 'GDPalt': 'NA', 'RGDP2alt': 'NA', 'debtgdp': 'NA',
              'GDP3': 'NA', 'GNI': 'NA', 'lRGDP': 'NA', 'lRGDP1': 'NA', 'lRGDP2': 'NA'}

ckan.client.action.datastore_upsert(resource_id=resource_id, force=True, records=[new_record], method='upsert')

# EXPECTED RESULTS
# The new record is added to the CKAN datasotre
internal_record = mongodb.db.get_collection(resource_id).find_one(new_record)
assert (internal_record is not None)

# A timestamp was added to the record that represents the point of time when the record was added to the resource
assert (internal_record['_created'] is not None)

# A timestamp '_valid_to' was attached to the older version of the record
old_record = mongodb.db.get_collection(resource_id).find_one({'id': 1, '_latest': False})
assert (old_record['_valid_to'] is not None)

logging.info("'tc2_2' successfully executed!")