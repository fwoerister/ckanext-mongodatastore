import pymongo

from ckanext.mongodatastore.util import normalize_json

TYPE_CONVERSION_DICT = {
    'string': str,
    'str': str,
    'text': str,
    'char': str,
    'integer': int,
    'int': int,
    'float': float,
    'number': float,
    'numeric': float,
    'bigint': int
}

NUMERIC_TYPES = [
    'integer',
    'int',
    'float',
    'number',
    'numeric',
    'bigint'
]

LOGICAL_OPERATORS = [
    '$and',
    '$or'
]


def transform_query_to_statement(query, schema):
    new_filter = {'$or': []}
    if type(query) == dict:
        for key in query.keys():
            new_filter['$or'].append({key: {'$regex': query[key]}})
        return new_filter

    for field in schema:
        if field['type'] == 'text':
            new_filter['$or'].append({field['id']: {'$regex': query}})
    return normalize_json(new_filter)


def transform_filter_to_statement(filters, schema):
    new_filter = {}
    schema_dict = {}

    for field in schema:
        schema_dict[field['id']] = field

    for key in filters.keys():
        if key in LOGICAL_OPERATORS:
            new_filter[key] = filters[key]
        elif type(filters[key]) is list:
            values = []
            if schema_dict[key]['type'] in NUMERIC_TYPES:
                for val in filters[key]:
                    try:
                        values.append(float(val))
                    except TypeError:
                        values.append(val)
            else:
                values = filters[key]

            new_filter[key] = {'$in': values}
        elif type(filters[key]) is dict:
            new_filter[key] = filters[key]
        else:
            if schema_dict[key]['type'] in NUMERIC_TYPES and type(filters[key]) == str:
                try:
                    if filters[key].startswith('<='):
                        value = TYPE_CONVERSION_DICT[schema_dict[key]['type']](filters[key][2:])
                        new_filter[key] = {'$lte': value}
                    elif filters[key].startswith('>='):
                        value = TYPE_CONVERSION_DICT[schema_dict[key]['type']](filters[key][2:])
                        new_filter[key] = {'$gte': value}
                    elif filters[key].startswith('<'):
                        value = TYPE_CONVERSION_DICT[schema_dict[key]['type']](filters[key][1:])
                        new_filter[key] = {'$lt': value}
                    elif filters[key].startswith('>'):
                        value = TYPE_CONVERSION_DICT[schema_dict[key]['type']](filters[key][1:])
                        new_filter[key] = {'$gt': value}
                    else:
                        new_filter[key] = TYPE_CONVERSION_DICT[schema_dict[key]['type']](filters[key])

                except TypeError:
                    new_filter[key] = filters[key]
            else:
                try:
                    new_filter[key] = TYPE_CONVERSION_DICT[schema_dict[key]['type']](filters[key])
                except TypeError:
                    new_filter[key] = filters[key]

    return normalize_json(new_filter)


def transform_projection(fields, schema):
    new_projection = {}

    if fields is None:
        fields = []

    if type(fields) is not list:
        fields = fields.split(',')

    for field in schema:
        if len(fields) == 0 or field['id'] in fields:
            new_projection[field['id']] = 1
    new_projection['_id'] = 0

    return normalize_json(new_projection)


def transform_sort(sort):
    if sort is None:
        sort = []

    if type(sort) is not list:
        sort = sort.split(',')

    transformed_sort = []
    for sort_arg in sort:
        if type(sort_arg) is dict:
            if sort_arg['order'] == 'asc':
                transformed_sort.append((sort_arg['field'], pymongo.ASCENDING))
            else:
                transformed_sort.append((sort_arg['field'], pymongo.DESCENDING))
        else:
            if sort_arg.split(' ')[-1] == 'asc':
                transformed_sort.append((sort_arg[0:-3].rstrip(), pymongo.ASCENDING))
            elif sort_arg.split(' ')[-1] == 'desc':
                transformed_sort.append((sort_arg[0:-4].rstrip(), pymongo.DESCENDING))
            else:
                transformed_sort.append((sort_arg, pymongo.ASCENDING))

    transformed_sort.append(('_id', pymongo.ASCENDING))
    return transformed_sort
