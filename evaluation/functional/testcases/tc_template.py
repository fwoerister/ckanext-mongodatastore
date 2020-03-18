import requests
from ckanapi import RemoteCKAN
from ckanapi import NotFound
from pymongo import MongoClient

CKAN_URL = 'http://localhost:5000'
CKAN_API_KEY = '302b24d4-8a23-47bd-baef-b8e8236d27a3'

MONGO_URL = 'mongodb://localhost:27017'

EVAL_USERNAME = 'evaluser'
EVAL_PASSWORD = 'passme123'

ckan_client = RemoteCKAN(CKAN_URL, apikey=CKAN_API_KEY)
mongo_client = MongoClient(MONGO_URL)
db = mongo_client.get_database('CKAN_Datastore')

# DESCRIPTION

# PRE-REQUISIT

# STEPS


# EXPECTED RESULTS
