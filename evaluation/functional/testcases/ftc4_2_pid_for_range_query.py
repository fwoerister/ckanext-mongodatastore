import logging
from time import sleep

import evaluation.util.ckan as ckan
import evaluation.util.env as env

# DESCRIPTION
# In this testcase a query is submitted to the datastore that retrieves all records where a specific
# field matches a range query. For the resulting dataset a PID is issued.

from evaluation.util import querystore, handle

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

logging.info("Start execution of 'tc4_2'")

# PRE-REQUISIT
env.verify_containers_are_running()
ckan.verify_if_evaluser_exists()
ckan.verify_if_organization_exists('tu-wien')
ckan.verify_package_does_exist('rr-experiment')
resource_id = ckan.verify_package_contains_resource('rr-experiment',
                                                    {'name': 'RR_processed.csv', 'datastore_active': True})

logging.info("pre-requisists are fullfilled")

# STEPS
results = []
pid = ckan.client.action.issue_pid(resource_id=resource_id, statement={'Country': 'Italy'}, sort='Infl asc')
results.append(ckan.client.action.querystore_resolve(pid=pid))

new_record = {'id': 1278, 'Country': 'Australia', 'Year': 2010, 'Debt': 101136.25205, 'RGDP': None, 'GDP': None,
              'dRGDP': 0.732249739168633, 'GDPI': 109.15168, 'GDP1': None, 'GDP2': 1201390, 'RGDP1': None,
              'RGDP2': 1100661, 'GDPI1': None, 'GDPI2': None, 'Infl': '1.629', 'Debt1': None, 'Debt2': None,
              'Debtalt': None, 'GDP2alt': None, 'GDPalt': None, 'RGDP2alt': None, 'debtgdp': 8.41826984160015,
              'GDP3': None, 'GNI': None, 'lRGDP': None, 'lRGDP1': None, 'lRGDP2': 1092660}
ckan.client.action.datastore_upsert(resource_id=resource_id, records=[new_record], method='insert', force=True)
results.append(ckan.client.action.querystore_resolve(pid=pid))

new_record = {'id': 1, 'Country': 'Australia', 'Year': 2000, 'Debt': None, 'RGDP': None, 'GDP': None,
              'dRGDP': None, 'GDPI': None, 'GDP1': None, 'GDP2': None, 'RGDP1': None,
              'RGDP2': None, 'GDPI1': None, 'GDPI2': None, 'Infl': None, 'Debt1': None, 'Debt2': None,
              'Debtalt': None, 'GDP2alt': None, 'GDPalt': None, 'RGDP2alt': None, 'debtgdp': None,
              'GDP3': None, 'GNI': None, 'lRGDP': None, 'lRGDP1': None, 'lRGDP2': None}

ckan.client.action.datastore_upsert(resource_id=resource_id, records=[new_record], method='upsert', force=True)
results.append(ckan.client.action.querystore_resolve(pid=pid))

ckan.client.action.datastore_delete(resource_id=resource_id, filters={'Country': 'Japan'}, force=True)
results.append(ckan.client.action.querystore_resolve(pid=pid))

logger.info("wait 5 seconds for background job to finish...")
sleep(5)

# EXPECTED RESULTS
ckan.verify_all_resultsets_are_equal(results)
handle_pid = querystore.verify_handle_was_assigned(pid)
handle.verify_handle_resolves_to_pid(handle_pid, pid)

logging.info("'tc4_2' successfully executed!")
