#!/bin/bash
set -e

pip install coverage
pip install -e 'git+https://github.com/ckan/ckan.git@ckan-2.8.5#egg=ckan'
pip install -r /home/travis/virtualenv/python2.7.17/src/ckan/requirements.txt
pip install -r requirements.txt

exit 0