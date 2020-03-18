import json
import logging

import xmltodict

from flask import request, Response, abort

from ckanext.mongodatastore.controller.mongodb_controller import VersionedDataStoreController

log = logging.getLogger(__name__)

CHUNK_SIZE = 10000


def generate_header(fields, delimiter):
    header = ''
    for field in fields:
        header += field + delimiter
    return header


def dump_dataset(pid):
    datastore_cntr = VersionedDataStoreController.get_instance()

    def to_csv():
        index = 0
        result = datastore_cntr.execute_stored_query(pid, index, CHUNK_SIZE, preview=True)
        records = list(result['records'])

        fields = [field['id'] for field in result['fields']]
        if csv_include_header:
            yield generate_header(fields, csv_delimiter) + '\n'

        while len(records) != 0:
            for record in records:
                yield csv_delimiter.join(map(lambda f: str(record[f]).replace('\n', ''), fields)) + '\n'
            result['records'].close()
            index = index + CHUNK_SIZE
            result = datastore_cntr.execute_stored_query(pid, index, CHUNK_SIZE, preview=True)
            records = list(result['records'])

    def to_json():
        index = 0
        result = datastore_cntr.execute_stored_query(pid, index, CHUNK_SIZE, preview=True)
        records = list(result['records'])

        yield '[\n'
        while len(records) != 0:
            for record in records:
                yield json.dumps(record) + ', \n'
            result['records'].close()
            index = index + CHUNK_SIZE
            result = datastore_cntr.execute_stored_query(pid, index, CHUNK_SIZE, preview=True)
            records = list(result['records'])
        yield ']'

    def to_xml():
        index = 0
        result = datastore_cntr.execute_stored_query(pid, index, CHUNK_SIZE, preview=True)
        records = list(result['records'])
        yield '<records>'
        while len(records) != 0:
            for record in records:
                yield xmltodict.unparse({'record': record}, full_document=False) + '\n'
            result['records'].close()
            index = index + CHUNK_SIZE
            result = datastore_cntr.execute_stored_query(pid, index, CHUNK_SIZE, preview=True)
            records = list(result['records'])
        yield '</records>'

    export_format = request.args.get('format', 'json')
    csv_delimiter = request.args.get('csvDelimiter', ';')
    csv_include_header = request.args.get('includeHeader', 'true') == 'true'

    r = None
    if export_format == 'csv':
        r = Response(to_csv(), mimetype='text/csv', content_type='application/octet-datadump')
        r.headers.set('Content-Disposition', 'attachment', filename='{0}.csv'.format(pid))
    elif export_format == 'xml':
        r = Response(to_xml(), mimetype='text/xml', content_type='application/octet-datadump')
        r.headers.set('Content-Disposition', 'attachment', filename='{0}.xml'.format(pid))
    elif export_format == 'json':
        r = Response(to_json(), mimetype='text/json', content_type='application/octet-datadump')
        r.headers.set('Content-Disposition', 'attachment', filename='{0}.json'.format(pid))

    if r:
        return r
    else:
        abort(405, 'Export format "{0}" not supported'.format(export_format))
