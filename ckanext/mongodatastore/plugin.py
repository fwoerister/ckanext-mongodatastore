import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckanext.datastore.interfaces import IDatastoreBackend
from flask import Blueprint

from ckanext.mongodatastore.datadump.datadump import dump_dataset
from ckanext.mongodatastore.datastore_backend import MongoDataStoreBackend
from ckanext.mongodatastore.logic.action import issue_query_pid, querystore_resolve
from ckanext.mongodatastore.util import urlencode


class MongodatastorePlugin(plugins.SingletonPlugin):
    plugins.implements(IDatastoreBackend)
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IRoutes, inherit=True)
    plugins.implements(plugins.IActions)
    plugins.implements(plugins.IBlueprint)
    plugins.implements(plugins.ITemplateHelpers)

    def update_config(self, config):
        toolkit.add_public_directory(config, 'theme/public')
        toolkit.add_template_directory(config, 'theme/templates')
        toolkit.add_resource('theme/public', 'ckanext-mongodatastore')

    # IDatastoreBackend
    def register_backends(self):
        return {
            u'mongodb': MongoDataStoreBackend,
            u'mongodb+srv': MongoDataStoreBackend,
        }

    # IRoutes
    def before_map(self, m):
        m.connect('querystore.view', '/querystore/view_query',
                  controller='ckanext.mongodatastore.controller.ui_controller:QueryStoreUIController',
                  action='view_history_query')

        m.connect('querystore.datadump', '/querystore/dump_history_result_set',
                  controller='ckanext.mongodatastore.controller.ui_controller:QueryStoreUIController',
                  action='dump_history_result_set')

        return m

    # IActions
    def get_actions(self):
        actions = {
            'issue_pid': issue_query_pid,
            'querystore_resolve': querystore_resolve
        }

        return actions

    def get_blueprint(self):
        blueprint = Blueprint(self.name, self.__module__)
        rules = [
            (u'/datadump/querystore_resolve/<int:pid>', u'querystore_resolve', dump_dataset),
        ]
        for rule in rules:
            blueprint.add_url_rule(*rule)

        return blueprint

    def get_helpers(self):
        return {'urlencode': urlencode}
