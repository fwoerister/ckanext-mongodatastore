import hashlib
import json
from collections import OrderedDict

from bson import ObjectId

CKAN_DATASTORE = 'CKAN_Datastore'

HASH_ALGORITHM = hashlib.md5


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        return json.JSONEncoder.default(self, o)


def normalize_json(json_data, max_depth=3):
    ordered_dict = OrderedDict()

    _normalize_json(json_data, ordered_dict, max_depth)

    return ordered_dict


def _normalize_json(obj, target, max_depth, id_on_first_pos=False):
    if id_on_first_pos:
        if 'id' in obj.keys():
            target['id'] = obj['id']
            obj.pop('id', None)

    for key in sorted(obj.keys()):
        if type(obj[key]) is dict:
            target[key] = OrderedDict()
            _normalize_json(obj[key], target[key], max_depth - 1)
        else:
            target[key] = obj[key]


def calculate_hash(data):
    encoder = JSONEncoder()
    algo = HASH_ALGORITHM()

    if type(data) == str:
        algo.update(data)
    else:
        for doc in data:
            algo.update(encoder.encode(doc).encode('utf-8'))

    return algo.hexdigest()
