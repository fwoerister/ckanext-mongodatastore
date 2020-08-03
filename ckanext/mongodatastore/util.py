import hashlib
import json
from collections import OrderedDict
import datetime
from json import JSONEncoder

import dateutil

HASH_ALGORITHM = hashlib.md5


class DateTimeEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return '[#convert_to_date]'+obj.isoformat()


def decodeDateTime(data):
    if type(data) == dict:
        for key in data.keys():
            if type(data[key]) in (str, unicode) and data[key].startswith('[#convert_to_date]'):
                data[key] = dateutil.parser.parse(data[key][18:])
    return data


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
    algo = HASH_ALGORITHM()

    if type(data) == str:
        algo.update(data)
    elif type(data) == dict:
        algo.update(json.dumps(data, default=str, sort_keys=True))
    elif type(data) == list:
        for doc in data:
            algo.update(json.dumps(doc, default=str, sort_keys=True))

    return algo.hexdigest()
