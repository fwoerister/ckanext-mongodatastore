import pymongo


def create_projection(schema, projection):
    new_projection = {}
    for field in schema:
        if len(projection) == 0 or field['id'] in projection.keys():
            new_projection[field['id']] = 1
    new_projection['_id'] = 0
    return new_projection


def transform_query_to_statement(query, schema):
    new_filter = {'$or': []}
    if type(query) == dict:
        for key in query.keys():
            new_filter['$or'].append({key: {'$regex': query[key]}})
        return new_filter

    for field in schema:
        if field['type'] == 'text':
            new_filter['$or'].append({field['id']: {'$regex': query}})
    return new_filter


def transform_filter(filters, schema):
    new_filter = {}

    schema_dict = {}
    for field in schema:
        schema_dict[field['id']] = field

    for key in filters.keys():
        if key in ['$and', '$or']:
            new_filter[key] = filters[key]
        elif type(filters[key]) is list:
            values = []

            if schema_dict[key]['type'] in ['number', 'numeric']:
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
            if schema_dict[key]['type'] in ['number', 'numeric'] and type(filters[key]) in (str, unicode):
                try:
                    if filters[key].startswith('<='):
                        value = float(filters[key][2:])
                        new_filter[key] = {'$lte': value}
                    elif filters[key].startswith('>='):
                        value = float(filters[key][2:])
                        new_filter[key] = {'$gte': value}
                    elif filters[key].startswith('<'):
                        value = float(filters[key][1:])
                        new_filter[key] = {'$lt': value}
                    elif filters[key].startswith('>'):
                        value = float(filters[key][1:])
                        new_filter[key] = {'$gt': value}
                    else:
                        new_filter[key] = float(filters[key])

                except TypeError:
                    new_filter[key] = filters[key]
            else:
                new_filter[key] = filters[key]
    return new_filter


def transform_sort(sort):
    transformed_sort = []
    for sort_arg in sort:
        if sort_arg.split(' ')[-1] == 'asc':
            transformed_sort.append((sort_arg[0:-3].rstrip(), pymongo.ASCENDING))
        elif sort_arg.split(' ')[-1] == 'desc':
            transformed_sort.append((sort_arg[0:-4].rstrip(), pymongo.DESCENDING))
        else:
            transformed_sort.append((sort_arg, pymongo.ASCENDING))
    return transformed_sort
