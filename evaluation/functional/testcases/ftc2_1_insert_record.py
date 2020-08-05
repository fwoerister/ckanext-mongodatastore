import logging

import evaluation.util.ckan as ckan
import evaluation.util.env as env
import evaluation.util.mongodb as mongodb

# DESCRIPTION
# Insert new records to an existing datastore resource.

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

logging.info("Start execution of 'tc2_1'")

# PRE-REQUISIT
env.verify_containers_are_running()
ckan.verify_if_evaluser_exists()
ckan.verify_if_organization_exists('tu-wien')

ckan.verify_package_does_exist('rr-experiment')
resource_id = ckan.verify_package_contains_resource('rr-experiment',
                                                    {'name': 'RR_processed.csv', 'datastore_active': True})

mongodb.remove_datastore_entries_by_id(resource_id, 1276)

logging.info("pre-requisists are fullfilled")

# STEPS
new_record = {'id': 1276, 'Country': 'Australia', 'Year': 2010, 'Debt': 101136.25205, 'RGDP': None, 'GDP': None,
              'dRGDP': 0.732249739168633, 'GDPI': 109.15168, 'GDP1': None, 'GDP2': 1201390, 'RGDP1': None,
              'RGDP2': 1100661, 'GDPI1': None, 'GDPI2': None, 'Infl': '1.629', 'Debt1': None, 'Debt2': None,
              'Debtalt': None, 'GDP2alt': None, 'GDPalt': None, 'RGDP2alt': None, 'debtgdp': 8.41826984160015,
              'GDP3': None, 'GNI': None, 'lRGDP': None, 'lRGDP1': None, 'lRGDP2': 1092660}

#  *) insert new record
ckan.client.action.datastore_upsert(resource_id=resource_id, force=True, records=[new_record], method='insert')

# EXPECTED RESULTS
mongodb.verify_new_document_is_in_mongo_collection(resource_id, new_record)
ckan.verify_new_record_is_in_datastore(resource_id, new_record)

logging.info("'tc2_1' successfully executed!")
