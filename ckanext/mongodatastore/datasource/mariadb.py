from ckanext.mongodatastore.datasource import DataSourceAdapter


class MariaDbDatasource(DataSourceAdapter):

    def get_primary_key_name(self, resource_url):
        pass

    def get_available_datasets(self, resource_url):
        pass

    def get_schema(self, resource_url, dataset):
        pass

    def migrate_records_to_datasource(self, dataset, resource_id, method):
        pass

    @staticmethod
    def get_protocol():
        return "mariadb"