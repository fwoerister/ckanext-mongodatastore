import logging

import click as click
from ckan.common import config as ckan_config
from sqlalchemy import create_engine

from ckanext.mongodatastore.controller.mongodb import VersionedDataStoreController
from ckanext.mongodatastore.model import Base

from datetime import datetime

log = logging.getLogger(__name__)


@click.group("mongodatastore")
def mongodatastore():
    u'''Perform commands to set up the querystore of the mongodatastore.
    '''


@mongodatastore.command('mongodatastore_create_schema')
@click.help_option(u'-h', u'--help')
def mongodatastore_init_querystore():
    # load_config(ctx.obj['config'])

    log.debug('start creating schema....')

    querystore_url = ckan_config[u'ckanext.mongodatastore.querystore_url']
    engine = create_engine(querystore_url, echo=True)

    Base.metadata.create_all(engine)
    log.debug('schema created!')

@mongodatastore.command('mongodatastore_check_integrity')
@click.help_option(u'-h', u'--help')
@click.pass_context
def mongodatastore_check_integrity(config=None):
    print(config)
    cntr = VersionedDataStoreController.get_instance()
    error_list = []

    start = datetime.utcnow()
    for id in cntr.querystore.get_cursor_on_ids():
        internal_id = int(id[0])
        if not cntr.execute_stored_query(internal_id, 0, 0, True):
            error_list.append(internal_id)
            print('query {0} is not valid!'.format(internal_id))
        else:
            print('query {0} is valid!'.format(internal_id))

    stop = datetime.utcnow()

    print('integrity check stopped after {0} seconds'.format((stop - start).total_seconds()))
    print('{0} problems detected'.format(len(error_list)))
    if len(error_list) > 0:
        print('The following PIDs do not retrieve a valid result set:')
        for internal_id in error_list:
            print(internal_id)

