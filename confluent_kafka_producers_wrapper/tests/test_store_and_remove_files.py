# Created by Antonio Di Mariano (antonio.dimariano@gmail.com) at 29/01/2021
from confluent_kafka_producers_wrapper.helpers.files_operations import store_schema
from confluent_kafka_producers_wrapper.helpers.files_operations import load_schema_from_file
from confluent_kafka_producers_wrapper.helpers.files_operations import remove_topic_schema
import unittest


class TestStoringAndRemovingFile(unittest.TestCase):

    def test_a_store_a_dict_to_file(self):
        to_match = {'https://user:paswd@myschemaregistry:17312': {'test-topic': {
            'value': {'type': 'record', 'name': 'example', 'namespace': 'example', 'doc': 'example',
                      'fields': [{'name': 'service_name', 'type': 'string'}, {'name': 'cloud', 'type': 'string'},
                                 {'name': 'timestamp', 'type': 'string'},
                                 {'name': 'template_shortname', 'type': 'string'},
                                 {'name': 'customer_email', 'type': 'string'},
                                 {'name': 'access_token', 'type': 'string'}, {'name': 'request_id', 'type': 'string'},
                                 {'name': 'request_source', 'type': 'string'},
                                 {'name': 'client_name', 'type': 'string'}, {'name': 'action_name', 'type': 'string'},
                                 {'name': 'action_datetime', 'type': 'string'}, {'name': 'results', 'type': 'string'}]},
            'key': {'type': 'record', 'name': 'example', 'namespace': 'example', 'doc': 'example',
                    'fields': [{'name': 'service_name', 'type': 'string'}]}}}}

        ret = store_schema(filename='../schemas_cache/test_file.json', topic='test-topic',
                           schema_registry='https://user:paswd@myschemaregistry:17312',
                           value_schema={"type": "record", "name": "example", "namespace": "example", "doc": "example",
                                         "fields": [{"name": "service_name", "type": "string"},
                                                    {"name": "cloud", "type": "string"},
                                                    {"name": "timestamp", "type": "string"},
                                                    {"name": "template_shortname", "type": "string"},
                                                    {"name": "customer_email", "type": "string"},
                                                    {"name": "access_token", "type": "string"},
                                                    {"name": "request_id", "type": "string"},
                                                    {"name": "request_source", "type": "string"},
                                                    {"name": "client_name", "type": "string"},
                                                    {"name": "action_name", "type": "string"},
                                                    {"name": "action_datetime", "type": "string"},
                                                    {"name": "results", "type": "string"}]},
                           key_schema={"type": "record", "name": "example", "namespace": "example", "doc": "example",
                                       "fields": [{"name": "service_name", "type": "string"}]})
        self.assertEqual(ret, to_match)

    def test_b_load_schema_from_file(self):
        to_match = {'value': {'type': 'record', 'name': 'example', 'namespace': 'example', 'doc': 'example',
                              'fields': [{'name': 'service_name', 'type': 'string'},
                                         {'name': 'cloud', 'type': 'string'}, {'name': 'timestamp', 'type': 'string'},
                                         {'name': 'template_shortname', 'type': 'string'},
                                         {'name': 'customer_email', 'type': 'string'},
                                         {'name': 'access_token', 'type': 'string'},
                                         {'name': 'request_id', 'type': 'string'},
                                         {'name': 'request_source', 'type': 'string'},
                                         {'name': 'client_name', 'type': 'string'},
                                         {'name': 'action_name', 'type': 'string'},
                                         {'name': 'action_datetime', 'type': 'string'},
                                         {'name': 'results', 'type': 'string'}]},
                    'key': {'type': 'record', 'name': 'example', 'namespace': 'example', 'doc': 'example',
                            'fields': [{'name': 'service_name', 'type': 'string'}]}}
        ret = load_schema_from_file(filename='../schemas_cache/test_file.json', topic='test-topic',
                                    schema_registry='https://user:paswd@myschemaregistry:17312')
        self.assertEqual(ret, to_match)

    def test_c_store_a_dict_to_file(self):
        to_match = {'https://user:paswd@myschemaregistry:17312': {'test-topic': {
            'value': {'type': 'record', 'name': 'example', 'namespace': 'example', 'doc': 'example',
                      'fields': [{'name': 'service_name', 'type': 'string'}, {'name': 'cloud', 'type': 'string'},
                                 {'name': 'timestamp', 'type': 'string'},
                                 {'name': 'template_shortname', 'type': 'string'},
                                 {'name': 'customer_email', 'type': 'string'},
                                 {'name': 'access_token', 'type': 'string'}, {'name': 'request_id', 'type': 'string'},
                                 {'name': 'request_source', 'type': 'string'},
                                 {'name': 'client_name', 'type': 'string'}, {'name': 'action_name', 'type': 'string'},
                                 {'name': 'action_datetime', 'type': 'string'}, {'name': 'results', 'type': 'string'}]},
            'key': {'type': 'record', 'name': 'example', 'namespace': 'example', 'doc': 'example',
                    'fields': [{'name': 'service_name', 'type': 'string'}]}}},
                    'https://user:paswd@myschemaregistry2:17312': {'test-topic': {
                        'value': {'type': 'record', 'name': 'example', 'namespace': 'example', 'doc': 'example',
                                  'fields': [{'name': 'service_name', 'type': 'string'},
                                             {'name': 'cloud', 'type': 'string'},
                                             {'name': 'timestamp', 'type': 'string'},
                                             {'name': 'template_shortname', 'type': 'string'},
                                             {'name': 'customer_email', 'type': 'string'},
                                             {'name': 'access_token', 'type': 'string'},
                                             {'name': 'request_id', 'type': 'string'},
                                             {'name': 'request_source', 'type': 'string'},
                                             {'name': 'client_name', 'type': 'string'},
                                             {'name': 'action_name', 'type': 'string'},
                                             {'name': 'action_datetime', 'type': 'string'},
                                             {'name': 'results', 'type': 'string'}]},
                        'key': {'type': 'record', 'name': 'example', 'namespace': 'example', 'doc': 'example',
                                'fields': [{'name': 'service_name', 'type': 'string'}]}}}}

        ret = store_schema(filename='../schemas_cache/test_file.json', topic='test-topic',
                           schema_registry='https://user:paswd@myschemaregistry2:17312',
                           value_schema={"type": "record", "name": "example", "namespace": "example", "doc": "example",
                                         "fields": [{"name": "service_name", "type": "string"},
                                                    {"name": "cloud", "type": "string"},
                                                    {"name": "timestamp", "type": "string"},
                                                    {"name": "template_shortname", "type": "string"},
                                                    {"name": "customer_email", "type": "string"},
                                                    {"name": "access_token", "type": "string"},
                                                    {"name": "request_id", "type": "string"},
                                                    {"name": "request_source", "type": "string"},
                                                    {"name": "client_name", "type": "string"},
                                                    {"name": "action_name", "type": "string"},
                                                    {"name": "action_datetime", "type": "string"},
                                                    {"name": "results", "type": "string"}]},
                           key_schema={"type": "record", "name": "example", "namespace": "example", "doc": "example",
                                       "fields": [{"name": "service_name", "type": "string"}]})
        self.assertEqual(ret, to_match)

    def test_d_load_schema_from_file(self):
        to_match = {'value': {'type': 'record', 'name': 'example', 'namespace': 'example', 'doc': 'example',
                              'fields': [{'name': 'service_name', 'type': 'string'},
                                         {'name': 'cloud', 'type': 'string'}, {'name': 'timestamp', 'type': 'string'},
                                         {'name': 'template_shortname', 'type': 'string'},
                                         {'name': 'customer_email', 'type': 'string'},
                                         {'name': 'access_token', 'type': 'string'},
                                         {'name': 'request_id', 'type': 'string'},
                                         {'name': 'request_source', 'type': 'string'},
                                         {'name': 'client_name', 'type': 'string'},
                                         {'name': 'action_name', 'type': 'string'},
                                         {'name': 'action_datetime', 'type': 'string'},
                                         {'name': 'results', 'type': 'string'}]},
                    'key': {'type': 'record', 'name': 'example', 'namespace': 'example', 'doc': 'example',
                            'fields': [{'name': 'service_name', 'type': 'string'}]}}
        ret = load_schema_from_file(filename='../schemas_cache/test_file.json', topic='test-topic',
                                    schema_registry='https://user:paswd@myschemaregistry2:17312')
        self.assertEqual(ret, to_match)

    def test_e_store_a_dict_to_file(self):
        to_match = {'https://user:paswd@myschemaregistry:17312': {'test-topic': {'value': {'type': 'record', 'name': 'example', 'namespace': 'example', 'doc': 'example', 'fields': [{'name': 'service_name', 'type': 'string'}, {'name': 'cloud', 'type': 'string'}, {'name': 'timestamp', 'type': 'string'}, {'name': 'template_shortname', 'type': 'string'}, {'name': 'customer_email', 'type': 'string'}, {'name': 'access_token', 'type': 'string'}, {'name': 'request_id', 'type': 'string'}, {'name': 'request_source', 'type': 'string'}, {'name': 'client_name', 'type': 'string'}, {'name': 'action_name', 'type': 'string'}, {'name': 'action_datetime', 'type': 'string'}, {'name': 'results', 'type': 'string'}]}, 'key': {'type': 'record', 'name': 'example', 'namespace': 'example', 'doc': 'example', 'fields': [{'name': 'service_name', 'type': 'string'}]}}}, 'https://user:paswd@myschemaregistry2:17312': {'test-topic': {'value': {'type': 'record', 'name': 'example', 'namespace': 'example', 'doc': 'example', 'fields': [{'name': 'service_name', 'type': 'string'}, {'name': 'cloud', 'type': 'string'}, {'name': 'timestamp', 'type': 'string'}, {'name': 'template_shortname', 'type': 'string'}, {'name': 'customer_email', 'type': 'string'}, {'name': 'access_token', 'type': 'string'}, {'name': 'request_id', 'type': 'string'}, {'name': 'request_source', 'type': 'string'}, {'name': 'client_name', 'type': 'string'}, {'name': 'action_name', 'type': 'string'}, {'name': 'action_datetime', 'type': 'string'}, {'name': 'results', 'type': 'string'}]}, 'key': {'type': 'record', 'name': 'example', 'namespace': 'example', 'doc': 'example', 'fields': [{'name': 'service_name', 'type': 'string'}]}}, 'new-topic': {'value': {'type': 'record', 'name': 'example', 'namespace': 'example', 'doc': 'example', 'fields': [{'name': 'service_name', 'type': 'string'}, {'name': 'cloud', 'type': 'string'}, {'name': 'timestamp', 'type': 'string'}, {'name': 'template_shortname', 'type': 'string'}, {'name': 'customer_email', 'type': 'string'}, {'name': 'access_token', 'type': 'string'}, {'name': 'request_id', 'type': 'string'}, {'name': 'request_source', 'type': 'string'}, {'name': 'client_name', 'type': 'string'}, {'name': 'action_name', 'type': 'string'}, {'name': 'action_datetime', 'type': 'string'}, {'name': 'results', 'type': 'string'}]}, 'key': {'type': 'record', 'name': 'example', 'namespace': 'example', 'doc': 'example', 'fields': [{'name': 'service_name', 'type': 'string'}]}}}}

        ret = store_schema(filename='../schemas_cache/test_file.json', topic='new-topic',
                           schema_registry='https://user:paswd@myschemaregistry2:17312',
                           value_schema={"type": "record", "name": "example", "namespace": "example", "doc": "example",
                                         "fields": [{"name": "service_name", "type": "string"},
                                                    {"name": "cloud", "type": "string"},
                                                    {"name": "timestamp", "type": "string"},
                                                    {"name": "template_shortname", "type": "string"},
                                                    {"name": "customer_email", "type": "string"},
                                                    {"name": "access_token", "type": "string"},
                                                    {"name": "request_id", "type": "string"},
                                                    {"name": "request_source", "type": "string"},
                                                    {"name": "client_name", "type": "string"},
                                                    {"name": "action_name", "type": "string"},
                                                    {"name": "action_datetime", "type": "string"},
                                                    {"name": "results", "type": "string"}]},
                           key_schema={"type": "record", "name": "example", "namespace": "example", "doc": "example",
                                       "fields": [{"name": "service_name", "type": "string"}]})
        self.assertEqual(ret, to_match)
    def test_f_delete_not_existing_schema_from_file(self):
        ret = remove_topic_schema(filename='../schemas_cache/test_file.json', topic='nonono',
                                  schema_registry='https://user:paswd@myschemaregistry:17312')
        self.assertEqual(ret, 0)
    def test_g_delete_existing_schema_from_file(self):
        ret = remove_topic_schema(filename='../schemas_cache/test_file.json', topic='test-topic',
                                  schema_registry='https://user:paswd@myschemaregistry2:17312')
        self.assertIsNotNone(ret)
