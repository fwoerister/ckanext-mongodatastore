import json
import unittest

from ckanext.mongodatastore.util import normalize_json, calculate_hash, encode_handle

FLAT_DICT = {
    'firstname': 'Florian',
    'lastname': 'Woerister',
    'id': 1
}

DEEP_DICT = {
    'firstname': 'Florian',
    'lastname': 'Woerister',
    'address': {
        'street': 'teststreet 123',
        'zip': '1234',
        'city': 'testcity',
        'country': 'at'
    },
    'mailaddresses': [
        'abc@abc.com',
        'xyz@xyz.com'
    ],
    'id': 1,
}


class TestNormalizeJson(unittest.TestCase):

    def test_normalize_flat_json(self):
        result = normalize_json(FLAT_DICT)
        expected_result = '{"firstname": "Florian", "id": 1, "lastname": "Woerister"}'

        assert json.dumps(result) == expected_result

    def test_normalize_deep_json(self):
        result = normalize_json(DEEP_DICT)
        expected_result = '{"address": {"city": "testcity", "country": "at", "street": "teststreet 123", "zip": "1234"}, "firstname": "Florian", "id": 1, "lastname": "Woerister", "mailaddresses": ["abc@abc.com", "xyz@xyz.com"]}'
        assert json.dumps(result) == expected_result


class TestHashCalculation(unittest.TestCase):

    def test_hash_of_unicode(self):
        data = u"Lorem ipsum dolor sit amet, consetetur sadipscing elitr, " \
               u"sed diam nonumy eirmod tempor invidunt ut labore et dolore " \
               u"magna aliquyam erat, sed diam voluptua. At vero eos et " \
               u"accusam et justo duo dolores et ea rebum."

        hash_value = calculate_hash(data)
        expected_hash = "8fc9e48edfa6fa591f0f48851f7a641a"

        assert hash_value == expected_hash

    def test_hash_of_string(self):
        data = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, " \
               "sed diam nonumy eirmod tempor invidunt ut labore et dolore " \
               "magna aliquyam erat, sed diam voluptua. At vero eos et " \
               "accusam et justo duo dolores et ea rebum."

        hash_value = calculate_hash(data)
        expected_hash = "8fc9e48edfa6fa591f0f48851f7a641a"

        assert hash_value == expected_hash

    def test_hash_of_list(self):
        data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

        hash_value = calculate_hash(data)
        expected_hash = "432f45b44c432414d2f97df0e5743818"

        assert hash_value == expected_hash

    def test_hash_of_dict(self):
        data = DEEP_DICT

        hash_value = calculate_hash(data)
        expected_hash = "1ea7ae18a60bceff308115b95031d3f1"

        assert hash_value == expected_hash


class TestUrlEncoding(unittest.TestCase):

    def test_url_encoding(self):
        handle_pid = "TEST/af7fb826-fcae-11ea-adc1-0242ac120002"

        result = encode_handle(handle_pid)
        expected_result = "TEST%2Faf7fb826--fcae--11ea--adc1--0242ac120002"

        assert result == expected_result
