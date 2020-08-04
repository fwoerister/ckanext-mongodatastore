#!/bin/sh -e

pip install -r requirements.txt
python setup.py install
python setup.py develop

cd evaluation
pip install -r requirements.txt

echo "wait for ckan to start up ..."
sleep 30

echo "start testing ..."
cd functional
./execute_all_tests.sh