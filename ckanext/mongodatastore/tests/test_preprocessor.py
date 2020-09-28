import json
import unittest

import pymongo

from ckanext.mongodatastore.preprocessor import transform_filter_to_statement, transform_query_to_statement, \
    transform_projection, transform_sort


class TestTransformStatement(unittest.TestCase):

    def test_transform_flat_statement(self):
        query = {'Year': '1991', 'Country': 'Austria'}
        schema = [{'id': 'Year', 'type': 'int'}, {'id': 'Country', 'type': 'text'}]

        transformed_query = transform_filter_to_statement(query, schema)

        expected_query = '{"Country": "Austria", "Year": 1991}'
        assert json.dumps(transformed_query) == expected_query

    def test_type_conversion(self):
        query = {'string_field': 1234,
                 'str_field': 1234,
                 'text_field': 1234,
                 'char_field': 1234,
                 'integer_field': '1234',
                 'int_field': '1234',
                 'float_field': '55.5',
                 'number_field': '55.5',
                 'numeric_field': '55.5',
                 'bigint_field': '100000000000000000000'
                 }

        schema = [
            {'id': 'string_field', 'type': 'string'},
            {'id': 'str_field', 'type': 'str'},
            {'id': 'text_field', 'type': 'text'},
            {'id': 'char_field', 'type': 'char'},
            {'id': 'integer_field', 'type': 'integer'},
            {'id': 'int_field', 'type': 'int'},
            {'id': 'float_field', 'type': 'float'},
            {'id': 'number_field', 'type': 'number'},
            {'id': 'numeric_field', 'type': 'numeric'},
            {'id': 'bigint_field', 'type': 'bigint'},
        ]

        transformed_query = transform_filter_to_statement(query, schema)

        expected_query = '{"bigint_field": 100000000000000000000, "char_field": "1234", "float_field": 55.5, "int_field": 1234, "integer_field": 1234, "number_field": 55.5, "numeric_field": 55.5, "str_field": "1234", "string_field": "1234", "text_field": "1234"}'

        assert type(transformed_query['string_field']) == str
        assert type(transformed_query['str_field']) == str
        assert type(transformed_query['text_field']) == str
        assert type(transformed_query['char_field']) == str
        assert type(transformed_query['integer_field']) == int
        assert type(transformed_query['int_field']) == int
        assert type(transformed_query['integer_field']) == int
        assert type(transformed_query['float_field']) == float
        assert type(transformed_query['number_field']) == float
        assert type(transformed_query['numeric_field']) == float
        assert type(transformed_query['bigint_field']) == int

        assert json.dumps(transformed_query) == expected_query

    def test_logical_operators(self):
        query = {'field': 'abc', '$or': [{'Country': 'Austria', 'Country': 'Italy'}]}

        schema = [{'id': 'field', 'type': 'text'}]

        transformed_statement = transform_filter_to_statement(query, schema)
        expected_query = '{"$or": [{"Country": "Italy"}], "field": "abc"}'

        assert json.dumps(transformed_statement) == expected_query


class TestTransformQuery(unittest.TestCase):

    def test_transform_query_on_all_fields(self):
        query = 'Aus'
        schema = [
            {'id': 'Field_1', 'type': 'text'},
            {'id': 'Field_2', 'type': 'int'},
            {'id': 'Field_3', 'type': 'text'},
            {'id': 'Field_4', 'type': 'bigint'},
            {'id': 'Field_5', 'type': 'text'},
        ]

        transformed_query = transform_query_to_statement(query, schema)
        expected_query = '{"$or": [{"Field_1": {"$regex": "Aus"}}, {"Field_3": {"$regex": "Aus"}}, {"Field_5": {"$regex": "Aus"}}]}'

        assert json.dumps(transformed_query) == expected_query

    def test_transform_query_on_specific_field(self):
        query = {'Field_1': 'Aus'}
        schema = [
            {'id': 'Field_1', 'type': 'text'},
            {'id': 'Field_2', 'type': 'int'},
            {'id': 'Field_3', 'type': 'text'},
            {'id': 'Field_4', 'type': 'bigint'},
            {'id': 'Field_5', 'type': 'text'},
        ]

        transformed_query = transform_query_to_statement(query, schema)
        expected_query = '{"$or": [{"Field_1": {"$regex": "Aus"}}]}'

        assert json.dumps(transformed_query) == expected_query


class TestTransformProjection(unittest.TestCase):

    def test_transform_string_projection(self):
        fields = 'Country,GDP,NOT_EXISTING_FIELD'

        schema = [
            {'id': 'Country', 'type': 'text'},
            {'id': 'GDP', 'type': 'numeric'}
        ]

        transformed_projection = transform_projection(fields, schema)

        expected_projection = '{"Country": 1, "GDP": 1, "_id": 0}'

        assert json.dumps(transformed_projection) == expected_projection

    def test_transform_list_projection(self):
        fields = ['Country', 'GDP', 'NOT_EXISTING_FIELD']

        schema = [
            {'id': 'Country', 'type': 'text'},
            {'id': 'GDP', 'type': 'numeric'}
        ]

        transformed_projection = transform_projection(fields, schema)

        expected_projection = '{"Country": 1, "GDP": 1, "_id": 0}'

        assert json.dumps(transformed_projection) == expected_projection


class TestTransformSort(unittest.TestCase):

    def test_transform_empty_sort(self):
        sort = None

        transformed_sort = transform_sort(sort)
        expected_sort = [('_id', pymongo.ASCENDING)]

        assert transformed_sort == expected_sort

    def test_transform_sort_with_sort_order(self):
        sort = 'Country,GDP desc,Year asc'

        transformed_sort = transform_sort(sort)
        expected_sort = [('Country', pymongo.ASCENDING), ('GDP', pymongo.DESCENDING),
                         ('Year', pymongo.ASCENDING), ('_id', pymongo.ASCENDING)]

        assert transformed_sort == expected_sort
