[![Build Status](https://travis-ci.org/fwoerister/ckanext-mongodatastore.svg?branch=master)](https://travis-ci.org/fwoerister/ckanext-mongodatastore)
[![PyPI version](https://badge.fury.io/py/ckanext-mongodatastore.svg)](https://badge.fury.io/py/ckanext-mongodatastore)
[![Supported Python versions](https://pypip.in/py_versions/ckanext-mongodatastore/badge.svg)](https://pypi.python.org/pypi/ckanext-mongodatastore/)
[![Development Status](https://pypip.in/status/ckanext-mongodatastore/badge.svg)](https://pypi.python.org/pypi/ckanext-mongodatastore/https://pypi.python.org/pypi/ckanext-mongodatastore/)

<img src="https://raw.githubusercontent.com/fwoerister/ckanext-mongodatastore/master/images/TU_Signet_SW_rgb.png" align="right" width="150px"/>

# ckanext-mongodatastore

This plugin provides a datastore implementation for [CKAN](https://www.ckan.org), based on [MongoDB](https://www.mongodb.org) for storing data records. One aspect of this implementation is, that it follows the [RDA Recommendations for Data Citation](https://doi.org/10.15497/RDA00016). This guarantiees citability for every query that is submited to the datastore.

<div style="text-align:center"><img src="https://raw.githubusercontent.com/fwoerister/ckanext-mongodatastore/master/images/BigPicture.png" align='center'/></div>

*As this extension provides an implementation of the* [IDatastoreBackend](https://docs.ckan.org/en/latest/maintaining/datastore.html#extending-datastore), *therefore the DataStore API can be used as before.*

## Requirements
This CKAN extension is tested with CKAN 2.9.0 running on Python 3.8.2.

To run this plugin beside an CKAN a mongo and a postgre database is required. The mongo database is needed for storing the data records and the postgre database is used as a querystore, described in the RDA Recommendations. For both instances a connection string has to be set in the CKAN config file.

## Installation

To install ckanext-mongodatastore:

1. Install [MongoDB](https://docs.mongodb.com/manual/installation/)

2. If not already existing, a QueryStore database has to be created::

```
sudo -u postgres createuser -S -D -R -P querystore
sudo -u postgres createdb -O querystore querystore -E utf-8
```

3. Activate your CKAN virtual environment, for example::

`. /usr/lib/ckan/default/bin/activate`

4. Install the ckanext-mongodatastore Python package into your virtual environment:

`pip install ckanext-mongodatastore`

5. Set the ckanext-mongodatastore specific config settings the CKAN configuration file 
   (by default the config file is located at
   ``/etc/ckan/default/production.ini``).

5. Add ``mongodatastore`` to the ``ckan.plugins`` setting in your CKAN
   config file (by default the config file is located at
   ``/etc/ckan/default/production.ini``).
   
6. Initialize the query store with the custom _click_ command `create_schema`
`ckan -c "/etc/ckan/default/production.ini" create_schema`

7. Restart CKAN. For example if you've deployed CKAN with Apache on Ubuntu::

`sudo service apache2 reload`

## Config Settings
For running the MongoDb datastore two settings have to configured in your CKAN's configuration file::

Name | Description | Default
--|--|--
`ckanext.mongodatastore.mongodb_url` | URL pointing to the MongoDB instance | 
`ckanext.mongodatastore.querystore_url` | URL pointing to the QueryStore database |
`ckanext.mongodatastore.sharding_enabled` | If a sharded MongoDB instance is used, the sharding feature has to be enabled | `False`
`ckanext.mongodatastore.database_name` | Name of the MongoDB database, that contains all resource collections | `CKAN_Datastore`

## Development Installation

To install ckanext-mongodatastore for development, activate your CKAN virtualenv and
do::

    git clone https://github.com/fwoerister/ckanext-mongodatastore.git
    cd ckanext-mongodatastore
    python setup.py develop
    pip install -r dev-requirements.txt

## Running the Tests

To run the tests, do::

    nosetests --nologcapture --with-pylons=test.ini

To run the tests and produce a coverage report, first make sure you have
coverage installed in your virtualenv (``pip install coverage``) then run::

    nosetests --nologcapture --with-pylons=test.ini --with-coverage --cover-package=ckanext.mongodatastore --cover-inclusive --cover-erase --cover-tests
