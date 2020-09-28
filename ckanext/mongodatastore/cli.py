import logging

import click as click
from ckan.common import config as ckan_config
# from ckan.lib.cli import load_config
from sqlalchemy import create_engine

from ckanext.mongodatastore.model import Base

# from datetime import datetime

log = logging.getLogger(__name__)


@click.group()
def mongodatastore():
    u'''Perform commands to set up the querystore of the mongodatastore.
    '''


@mongodatastore.command('create_schema')
@click.help_option(u'-h', u'--help')
def init_querystore():
    # load_config(ctx.obj['config'])

    log.debug('start creating schema....')

    querystore_url = ckan_config[u'ckanext.mongodatastore.querystore_url']
    engine = create_engine(querystore_url, echo=True)

    Base.metadata.create_all(engine)
    log.debug('schema created!')

# @click.help_option(u'-h', u'--help')
# @click.pass_context
# def check_integrity(ctx, config):
#     load_config(config or ctx.obj['config'])
#
#     cntr = VersionedDataStoreController.get_instance()
#     error_list = []
#
#     start = datetime.utcnow()
#     for pid in cntr.querystore.get_cursor_on_ids():
#         if not cntr.execute_stored_query(pid, 0, 0, True):
#             error_list.append(pid)
#             print('query {0} is not valid!'.format(pid))
#         else:
#             print('query {0} is valid!'.format(pid))
#
#     stop = datetime.utcnow()
#
#     print('integrity check stopped after {0} seconds'.format((stop - start).total_seconds()))
#     print('{0} problems detected'.format(len(error_list)))
#     if len(error_list) > 0:
#         print('The following PIDs do not retrieve a valid result set:')
#         for pid in error_list:
#             print(pid)
