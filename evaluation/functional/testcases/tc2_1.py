import evaluation.util.ckan as ckan
import evaluation.util.env as env
import evaluation.util.mongodb as mongodb

# DESCRIPTION
# Insert new records to an existing datastore resource.

# PRE-REQUISIT
env.verify_containers_are_running()
ckan.verify_if_evaluser_exists()
ckan.verify_if_organization_exists('tu-wien')

ckan.verify_package_does_exist('rr-experiment')
resource_id = ckan.verify_package_contains_resource('rr-experiment',
                                                    {'name': 'RR_processed.csv', 'datastore_active': True})

mongodb.remove_datastore_entries_by_id(resource_id, 1276)

# STEPS
new_record = {'id': 1276, 'Country': 1, 'Year': 2010, 'Debt': '101136.25205', 'RGDP': 'NA', 'GDP': 'NA',
              'dRGDP': '0.732249739168633', 'GDPI': '109.15168', 'GDP1': 'NA', 'GDP2': '1201390', 'RGDP1': 'NA',
              'RGDP2': '1100661', 'GDPI1': 'NA', 'GDPI2': 'NA', 'Infl': '1.629', 'Debt1': 'NA', 'Debt2': 'NA',
              'Debtalt': 'NA', 'GDP2alt': 'NA', 'GDPalt': 'NA', 'RGDP2alt': 'NA', 'debtgdp': '8.41826984160015',
              'GDP3': 'NA', 'GNI': 'NA', 'lRGDP': 'NA', 'lRGDP1': 'NA', 'lRGDP2': '1092660'}

#  *) insert new record
ckan.client.action.datastore_upsert(resource_id=resource_id, force=True, records=[new_record], method='insert')

# EXPECTED RESULTS
mongodb.verify_new_document_is_in_mongo_collection(resource_id, new_record)
ckan.verify_new_record_is_in_datastore(resource_id, new_record)
