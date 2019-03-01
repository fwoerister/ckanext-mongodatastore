from ckan import plugins

from ckanext.mongodatastore.interfaces import IDataSourceAdapter


class DataSourceAdapter:
    def __init__(self):
        pass

    _adapters = {}

    @classmethod
    def register_datasource(cls):
        """Register all datasource adapter implementations inside extensions.
        """
        for plugin in plugins.PluginImplementations(IDataSourceAdapter):
            cls._adapters.update(plugin.register_datasource())

    @classmethod
    def get_datasource_adapter(cls, url):
        return cls._adapters[url.split(':')[0]](url)

    def is_reachable(self):
        raise NotImplementedError()

    def get_primary_key_name(self, resource_url):
        raise NotImplementedError()

    def get_available_datasets(self, resource_url):
        raise NotImplementedError()

    def get_schema(self, resource_url, dataset):
        raise NotImplementedError()

    def migrate_records_to_datasource(self, dataset, resource_id, method):
        raise NotImplementedError()

    @staticmethod
    def get_protocol():
        raise NotImplementedError
