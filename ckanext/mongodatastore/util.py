import hashlib
import json
from collections import OrderedDict

HASH_ALGORITHM = hashlib.md5


def normalize_json(json_data, max_depth=3):
    ordered_dict = OrderedDict()

    __normalize_json_rec(json_data, ordered_dict, max_depth)

    return ordered_dict


def __normalize_json_rec(obj, target, max_depth, id_on_first_pos=False):
    if id_on_first_pos:
        if 'id' in obj.keys():
            target['id'] = obj['id']
            obj.pop('id', None)

    for key in sorted(obj.keys()):
        if type(obj[key]) is dict:
            target[key] = OrderedDict()
            __normalize_json_rec(obj[key], target[key], max_depth - 1)
        else:
            target[key] = obj[key]


def calculate_hash(data):
    algo = HASH_ALGORITHM()

    if type(data) == str:
        algo.update(data.encode('utf-8'))
    elif type(data) == dict:
        algo.update(json.dumps(data, default=str, sort_keys=True, ensure_ascii=False).encode('utf-8'))
    elif type(data) == list:
        for doc in data:
            algo.update(json.dumps(doc, default=str, sort_keys=True, ensure_ascii=False).encode('utf-8'))

    return algo.hexdigest()


def encode_handle(s):
    return s.replace('/','%2F').replace('-', '--')
