#!/bin/sh
set -e

# URL for the primary database, in the format expected by sqlalchemy (required
# unless linked to a container called 'db')
: ${CKAN_SQLALCHEMY_URL:=}
# URL for solr (required unless linked to a container called 'solr')
: ${CKAN_SOLR_URL:=}
# URL for redis (required unless linked to a container called 'redis')
: ${CKAN_REDIS_URL:=}
# URL for datapusher (required unless linked to a container called 'datapusher')
: ${CKAN_DATAPUSHER_URL:=}

CONFIG="${CKAN_CONFIG}/production.ini"

export CKAN_SITE_ID=${CKAN_SITE_ID}
export CKAN_SITE_URL=${CKAN_SITE_URL}
export CKAN_SQLALCHEMY_URL=${CKAN_SQLALCHEMY_URL}
export CKAN_SOLR_URL=${CKAN_SOLR_URL}
export CKAN_REDIS_URL=${CKAN_REDIS_URL}
export CKAN_STORAGE_PATH=/var/lib/ckan
export CKAN_DATAPUSHER_URL=${CKAN_DATAPUSHER_URL}
export CKAN_DATASTORE_PG_WRITE_URL=${CKAN_DATASTORE_PG_WRITE_URL}
export CKAN_DATASTORE_MG_WRITE_URL=${CKAN_DATASTORE_MG_WRITE_URL}
export CKAN_DATASTORE_PG_READ_URL=${CKAN_DATASTORE_PG_READ_URL}
export CKAN_DATASTORE_MG_READ_URL=${CKAN_DATASTORE_MG_READ_URL}
export CKAN_DATASTORE_DATABASE=${CKAN_DATASTORE_DATABASE}
export CKAN_QUERYSTORE_URL=${CKAN_QUERYSTORE_URL}
export CKAN_PLUGINS=${CKAN_PLUGINS}

export PGPASSWORD=ckan

# setup configuration file
rm -f "$CONFIG"

echo "wait for mongodb initialization...."
sleep 30

ckan-paster make-config --no-interactive ckan "$CONFIG"

ckan-pip install flask_debugtoolbar --upgrade

ckan-paster --plugin=ckan config-tool "$CONFIG" -e \
  "sqlalchemy.url = ${CKAN_SQLALCHEMY_URL}" \
  "solr_url = ${CKAN_SOLR_URL}" \
  "ckan.storage_path = /var/lib/ckan" \
  "ckan.datastore.write_url = ${CKAN_DATASTORE_MG_WRITE_URL}" \
  "ckan.datastore.read_url = ${CKAN_DATASTORE_MG_READ_URL}" \
  "ckan.site_url = ${CKAN_SITE_URL}" \
  "ckan.site_id = ${CKAN_SITE_ID}" \
  "ckan.plugins = stats text_view image_view recline_view datapusher datastore mongodatastore"

sed "/\[app:main\]/a ckan.querystore.url=${CKAN_QUERYSTORE_URL}" "$CONFIG" -i.bkp
sed "/\[app:main\]/a ckan.querystore.url=${CKAN_QUERYSTORE_URL}" "$CONFIG" -i.bkp
sed "/\[app:main\]/a ckan.datastore.database=${CKAN_DATASTORE_DATABASE}" "$CONFIG" -i.bkp
sed "/\[app:main\]/a ckan.jobs.timeout=600" "$CONFIG" -i.bkp

cd /usr/lib/ckan/venv/src/ckan
# start worker
ckan-paster --plugin=ckan jobs worker hash_queue --config=/etc/ckan/production.ini &

# init databases
ckan-paster --plugin=ckan db init -c "${CKAN_CONFIG}/production.ini"

# install mongodatastore
cd /usr/lib/ckan/venv/src/ckanext-mongodatastore
ckan-paster --plugin=ckan querystore create_schema -c "${CKAN_CONFIG}/production.ini"


# evaluser:passme123
psql -U ckan -h db -d ckan -c "INSERT INTO public.user (id, name, apikey, created, about, password, fullname, email, reset_key, sysadmin, activity_streams_email_notifications, state) VALUES ('ff7f6a70-605f-4ad2-b70c-7d70b0fb6c30', 'evaluser', '302b24d4-8a23-47bd-baef-b8e8236d27a3', '2020-03-20 11:11:50.794895', null, '\$pbkdf2-sha512\$25000\$fm8N4VyrtbZWyhnDWEspZQ\$PUKHskPZazAKOMOaM/WMCG7q7DHAQExf9ux.K.QyGFmXxQ.7UDPQsY3b6qgQHD3wOSEl5lSKLKlzhBZBmJGPCw', null, 'evaluser@localhost', null, true, false, 'active') on conflict do nothing;"
psql -U ckan -h db -d ckan -c "INSERT INTO public.revision (id, timestamp, author, message, state, approved_timestamp) VALUES ('56318b3e-f0d6-45cc-bd16-d3a9077ad6fb', '2020-03-23 09:11:49.620416', 'evaluser', '', 'active', null) on conflict do nothing;"
psql -U ckan -h db -d ckan -c "INSERT INTO public.group (id, name, title, description, created, state, revision_id, type, approval_status, image_url, is_organization) VALUES ('dc13c7c9-c3c9-42ac-8200-8fe007c049a1', 'tu-wien', 'TU Wien', '', '2020-03-23 09:11:49.634273', 'active', '56318b3e-f0d6-45cc-bd16-d3a9077ad6fb', 'organization', 'approved', 'https://upload.wikimedia.org/wikipedia/commons/8/8f/TU_Logo.svg', true) on conflict do nothing;"
psql -U ckan -h db -d ckan -c "INSERT INTO public.group_revision (id, name, title, description, created, state, revision_id, continuity_id, expired_id, revision_timestamp, expired_timestamp, current, type, approval_status, image_url, is_organization) VALUES ('dc13c7c9-c3c9-42ac-8200-8fe007c049a1', 'tu-wien', 'TU Wien', '', '2020-03-23 09:11:49.634273', 'active', '56318b3e-f0d6-45cc-bd16-d3a9077ad6fb', 'dc13c7c9-c3c9-42ac-8200-8fe007c049a1', null, '2020-03-23 09:11:49.620416', '9999-12-31 00:00:00.000000', null, 'organization', 'approved', 'https://upload.wikimedia.org/wikipedia/commons/8/8f/TU_Logo.svg', true) on conflict do nothing;"

cd /usr/lib/ckan/venv/src/ckan

# start ckan
ckan-paster serve "${CKAN_CONFIG}/production.ini"
