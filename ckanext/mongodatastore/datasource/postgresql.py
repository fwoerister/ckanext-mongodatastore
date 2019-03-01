from ckan.common import c
from ckan.logic import get_action
from sqlalchemy import create_engine

from ckanext.mongodatastore.datasource import DataSourceAdapter

VERBOSE_MODE = False
PAGINATE_BY = 100


class PostgreSqlDatasource(DataSourceAdapter):

    def __init__(self, resource_url):
        self.resource_url = resource_url
        self.engine = create_engine(resource_url, echo=VERBOSE_MODE)

    def is_reachable(self):
        try:
            self.get_available_datasets()
        except Exception:
            return False
        return True

    def get_primary_key_name(self, dataset):
        result = self.engine.execute(("SELECT pg_attribute.attname as name "
                                      "FROM pg_index, pg_class, pg_attribute, pg_namespace "
                                      "WHERE pg_class.oid = '{0}'::regclass "
                                      "AND indrelid = pg_class.oid "
                                      "AND nspname = 'public' "
                                      "AND pg_class.relnamespace = pg_namespace.oid "
                                      "AND pg_attribute.attrelid = pg_class.oid "
                                      "AND pg_attribute.attnum = any(pg_index.indkey) "
                                      "AND indisprimary").format(dataset))

        primary_keys = []
        for key_name in result:
            primary_keys.append(key_name)

        assert len(primary_keys) == 1
        return primary_keys[0][0]

    def get_available_datasets(self):
        public_tables = []

        for rec in self.engine.execute("select table_name "
                                       "from information_schema.tables "
                                       "where table_schema like 'public'"):
            public_tables.append(rec[0])

        return public_tables

    def get_schema(self, dataset):

        schema = []

        for rec in self.engine.execute(("select column_name, data_type "
                                        "from information_schema.columns "
                                        "where table_schema = 'your_schema' "
                                        "and table_name   = 'your_table'").format()):
            schema.append({'id': rec[0], 'data_type': rec[1]})

        return schema

    def migrate_records_to_datasource(self, dataset, resource_id, method):
        if method not in ['upsert']:
            raise NotImplementedError()

        result = self.engine.execute('select count(*) from {0}'.format(dataset))
        count = list(result)[0][0]

        offset = 0
        while offset < count:
            query = 'select json_agg(t) from (select * from {0} offset  {1} limit {2}) as t'
            result = self.engine.execute(query.format(dataset, offset, PAGINATE_BY))

            records = list(result)[0][0]

            c.pkg_dict = get_action('datastore_upsert')(
                None, {'resource_id': resource_id,
                       'force': True,
                       'records': records,
                       'method': method,
                       'calculate_record_count': False})
            offset += PAGINATE_BY

    @staticmethod
    def get_protocol():
        return "postgresql"
