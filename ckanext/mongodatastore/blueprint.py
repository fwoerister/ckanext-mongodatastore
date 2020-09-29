import json

import ckan.plugins.toolkit as toolkit
import xmltodict
from flask import Blueprint, request, Response, abort

from ckanext.mongodatastore.controller.mongodb import VersionedDataStoreController
from ckanext.mongodatastore.exceptions import QueryNotFoundException

bp = Blueprint('storedquery', __name__, url_prefix='/storedquery')


@bp.route('/landingpage', methods=['GET'])
def render_landing_page():
    internal_id = request.args.get('id')
    result = None
    try:
        result = toolkit.get_action('querystore_resolve')(None, {'id': internal_id,
                                                                 'skip': 0,
                                                                 'limit': 100})
    except QueryNotFoundException as ex:
        abort(404, 'Unfortunately there is no entry with pid {0} in the query store!'.format(internal_id))

    count = 0
    if result['records']:
        count = len(result['records'])

    return toolkit.render('mongodatastore/query_view.html', extra_vars={'query': result['query'],
                                                                        'meta': result['meta'],
                                                                        'result_set': result['records'],
                                                                        'count': count,
                                                                        'projection': result['fields']})


CHUNK_SIZE = 10000


def generate_header(fields, delimiter):
    header = ''
    for field in fields:
        header += field + delimiter
    return header


@bp.route('/<int:internal_id>/dump', methods=['GET'])
def dump_query(internal_id):
    datastore_cntr = VersionedDataStoreController.get_instance()

    def convert_csv_field(value):
        if value:
            return str(value).replace('\n', '')
        return ''

    def to_csv():
        index = 0
        result = datastore_cntr.execute_stored_query(internal_id, index, CHUNK_SIZE, include_data=True)
        records = list(result['records'])

        fields = [field['id'] for field in result['fields']]
        if csv_include_header:
            yield generate_header(fields, csv_delimiter) + '\n'

        while len(records) != 0:
            for record in records:
                yield csv_delimiter.join(map(lambda f: convert_csv_field(record[f]), fields)) + '\n'
            index = index + CHUNK_SIZE
            result = datastore_cntr.execute_stored_query(internal_id, index, CHUNK_SIZE, include_data=True)
            records = list(result['records'])

    def to_json():
        index = 0
        result = datastore_cntr.execute_stored_query(internal_id, index, CHUNK_SIZE, include_data=True)
        records = list(result['records'])

        yield '[\n'
        while len(records) != 0:
            for record in records:
                yield json.dumps(record) + ', \n'
            index = index + CHUNK_SIZE
            result = datastore_cntr.execute_stored_query(internal_id, index, CHUNK_SIZE, include_data=True)
            records = list(result['records'])
        yield ']'

    def to_xml():
        index = 0
        result = datastore_cntr.execute_stored_query(internal_id, index, CHUNK_SIZE, include_data=True)
        records = list(result['records'])
        yield '<records>'
        while len(records) != 0:
            for record in records:
                yield xmltodict.unparse({'record': record}, full_document=False) + '\n'
            index = index + CHUNK_SIZE
            result = datastore_cntr.execute_stored_query(internal_id, index, CHUNK_SIZE, include_data=True)
            records = list(result['records'])
        yield '</records>'

    export_format = request.args.get('format', 'json')
    csv_delimiter = request.args.get('csvDelimiter', ';')
    csv_include_header = request.args.get('includeHeader', 'true').lower() == 'true'

    if export_format == 'csv':
        r = Response(to_csv(), mimetype='text/csv', content_type='application/octet-datadump')
        r.headers.set('Content-Disposition', 'attachment', filename='{0}.csv'.format(internal_id))
        return r
    elif export_format == 'xml':
        r = Response(to_xml(), mimetype='text/xml', content_type='application/octet-datadump')
        r.headers.set('Content-Disposition', 'attachment', filename='{0}.xml'.format(internal_id))
        return r
    elif export_format == 'json':
        r = Response(to_json(), mimetype='text/json', content_type='application/octet-datadump')
        r.headers.set('Content-Disposition', 'attachment', filename='{0}.json'.format(internal_id))
        return r

    abort(405, 'Export format "{0}" not supported'.format(export_format))
