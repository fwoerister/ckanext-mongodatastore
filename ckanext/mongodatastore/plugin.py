import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckanext.datastore.interfaces import IDatastoreBackend

from ckanext.mongodatastore import blueprint
from ckanext.mongodatastore.cli import init_querystore
from ckanext.mongodatastore.datastore_backend import MongoDataStoreBackend
from ckanext.mongodatastore.logic.action import issue_query_pid, querystore_resolve, nonversioned_query
from ckanext.mongodatastore.util import urlencode


class MongodatastorePlugin(plugins.SingletonPlugin):
    plugins.implements(IDatastoreBackend)
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IRoutes, inherit=True)
    plugins.implements(plugins.IActions)
    plugins.implements(plugins.IBlueprint)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.IClick)

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

    # IActions
    def get_actions(self):
        actions = {
            'issue_pid': issue_query_pid,
            'querystore_resolve': querystore_resolve,
            'nv_query': nonversioned_query
        }

        return actions

    # IBlueprint
    def get_blueprint(self):
        return blueprint.bp

    # ITemplateHelpers
    def get_helpers(self):
        return {'urlencode': urlencode}

    # IClick
    def get_commands(self):
        return [init_querystore]
