#!/bin/bash
set -e

pip install coverage
pip install -e 'git+https://github.com/ckan/ckan.git@ckan-2.9.0#egg=ckan'
pip install -r /home/travis/virtualenv/python3.8.2/src/ckan/requirements.txt
pip install -r requirements.txt

exit 0