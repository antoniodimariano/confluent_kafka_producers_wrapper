# Created by Antonio Di Mariano (antonio.dimariano@gmail.com) at 08/01/2021
import json
import avro
from confluent_kafka_producers_wrapper.helpers.files_operations import store_schema
from confluent_kafka_producers_wrapper.helpers.files_operations import load_schema_from_file
import logging

logger = logging.getLogger()


class SchemaRegistryDriver:

    def __make_call_to_the_schema_registry(self, avro_subject_to_query, headers, auth=None):
        """
        This method sends a REST request to the given avro_subject_to_query
        :param avro_subject_to_query:
        :param headers:
        :param auth:
        :return:
        """
        try:
            from confluent_kafka_producers_wrapper.communication.RESTClient import RestClient
            return RestClient().send_rest_request(method='GET', service_url=avro_subject_to_query, headers=headers,
                                                  auth=auth)

        except Exception as error:
            logger.error("EXCEPTION %s occurred contacting the schema registry %s"
                         % (error, avro_subject_to_query))
            return 0

    def get_schema_versions_for_topic(self, topic, schema_registry, http_basic_auth=None):
        """
        This method sends a GET request to schema_registry/subjects/topic/versions
        to get the list of all the available versions for the given topic.
        It returns the list of all available versions and the latest one for the given topic/
        :param topic:
        :param schema_registry:
        :param http_basic_auth:
        :return:
        """
        try:

            list_of_available_versions = self.__make_call_to_the_schema_registry(
                avro_subject_to_query="{}/subjects/{}/versions".format(schema_registry, topic),
                headers={
                    "Content-Type": "application/vnd.schemaregistry.v1+json"}, auth=http_basic_auth)
            if list_of_available_versions:
                return list_of_available_versions.get('response'), list_of_available_versions.get('response')[-1]
            else:
                logger.debug(
                    'NO schema was found in the schema registry %s for topic %s \n' % (schema_registry, topic))
                return 0
        except Exception as error:
            logger.error(
                'EXCEPTION %s occurred contacting the schema registry %s to get versions for topic %s \n' % (
                    error, schema_registry, topic))

            return 0

    def get_latest_schema_for_topic(self, topic, schema_registry, http_basic_auth=None):
        """

        This method makes two request to the SCHEMA_REGISTRY in order
        to get the latest schema version of the given topic.
        The first request lists all the available versions. The second one is the one
        to get the content of the latest schema version.


        :param topic:
        :param schema_registry:
        :param http_basic_auth:
        :return:
        """
        try:

            list_of_available_versions, latest_version = self.get_schema_versions_for_topic(topic=topic,
                                                                                            schema_registry=schema_registry,
                                                                                            http_basic_auth=http_basic_auth)

            if list_of_available_versions:
                latest_schema_request = self.__make_call_to_the_schema_registry(
                    avro_subject_to_query="{}/subjects/{}/versions/{}".format(schema_registry, topic,
                                                                              latest_version),
                    headers={
                        "Content-Type": "application/vnd.schemaregistry.v1+json",
                    },
                    auth=http_basic_auth)
                if list_of_available_versions:
                    schema_as_json = json.loads(latest_schema_request.get('response').get("schema"))
                    return schema_as_json
                else:
                    logger.error(
                        'An error occurred getting the schema version %d for the topic %s from the schema registry %s  \n' % (
                            latest_version, schema_registry, topic))

            return 0
        except Exception as error:
            logger.error(
                'EXCEPTION %s occurred getting the latest schema for topic %s from schema registry %s \n' % (
                    error, topic, schema_registry))

            return 0

    def load_key_and_value_from_local_cache_or_get_from_schema_registry_and_store(self, topic, schema_registry,
                                                                                  basic_auth_user_info):
        """
        This method checks if a schema is stored in the local cache schemas_cache/topics_schemas.json.
        If a schema is found, no queries to the schema_registry are needed.
        Otherwise, a call to the schema_registry is sent and the result stored in the schemas_cache/topics_schemas.json.

        :param topic:
        :param schema_registry:
        :param basic_auth_user_info:
        :return:
        """
        try:

            stored_schema = load_schema_from_file(topic=topic, schema_registry=schema_registry)
            if stored_schema:
                # schemas found stored in the local cache
                logger.debug('Cached value/schema found for topic %s %s. \n' % (
                    topic, schema_registry))

                value_schema = stored_schema.get('value')
                key_schema = stored_schema.get('key')

            else:
                # no schemas are stored for the given topic and a query to the schema_registry will be sent.
                key_schema, value_schema = self.get_latest_schema_value_and_key_for_the_topic(basic_auth_user_info,
                                                                                              schema_registry,
                                                                                              topic)
                if value_schema and key_schema:
                    logger.debug(
                        'value/key schema fetched for topic %s %s. \n' % (
                            topic, schema_registry))
                    # the just received schemas is going to be stored.
                    store_schema(topic=topic, key_schema=key_schema, value_schema=value_schema,
                                 schema_registry=schema_registry)
                else:
                    logger.error(
                        'ERROR occurred fetching the value/schema for topic %s %s. NO value and schema fetched/available \n' % (
                            topic, schema_registry))
                    return 0
            return key_schema, value_schema
        except Exception as error:
            logger.error(
                'EXCEPTION %s occurred fetching the value/schema for topic %s %s. NO value and schema fetched/available \n' % (
                    error,topic, schema_registry))
            return 0

    def get_key_schema_and_value_schema(self, topic, schema_registry, basic_auth_user_info=None,
                                        store_and_load_schema=1, verbose=0):
        """
        This method returns the value and the key schema for the given topic
        :param verbose:
        :param topic:
        :param schema_registry:
        :param basic_auth_user_info:
        :return:
        """
        try:
            if store_and_load_schema:
                if verbose:
                    logger.debug(
                        'Cache option required for  the value/schema of topic %s %s. \n' % (
                            topic, schema_registry))
                key_schema, value_schema = self.load_key_and_value_from_local_cache_or_get_from_schema_registry_and_store(
                    basic_auth_user_info=basic_auth_user_info, schema_registry=schema_registry, topic=topic)

            else:

                key_schema, value_schema = self.get_latest_schema_value_and_key_for_the_topic(
                    basic_auth_user_info=basic_auth_user_info, schema_registry=schema_registry, topic=topic)

            return {
                "topic_key_schema": avro.schema.parse(json.dumps(key_schema)),
                "topic_value_schema": avro.schema.parse(json.dumps(value_schema)),
            }

        except Exception as error:
            logger.error(
                'Exception %s occurred fetching the value/schema for topic %s %s. NO value and schema fetched/available \n' % (
                    error, topic, schema_registry))

            return 0

    def get_latest_schema_value_and_key_for_the_topic(self, basic_auth_user_info, schema_registry, topic):
        """
        If the BASIC_AUTH is not False it means that it is a dictionary with `username` and `password` as keys.
        They are required when we work with a Confluent Cloud brokers.
        :param basic_auth_user_info:
        :param schema_registry:
        :param topic:
        :return:
        """
        http_basic_auth = None
        if basic_auth_user_info:
            http_basic_auth = {"username": basic_auth_user_info.split(':')[0],
                               "password": basic_auth_user_info.split(':')[1]}
            from requests.auth import HTTPBasicAuth
            # set the HTTP Basic Auth.
            http_basic_auth = HTTPBasicAuth(http_basic_auth.get('username', None),
                                            http_basic_auth.get('password', None))

        value_schema = self.get_latest_schema_for_topic(topic=topic + "-value",
                                                        schema_registry=schema_registry,
                                                        http_basic_auth=http_basic_auth)
        key_schema = self.get_latest_schema_for_topic(topic=topic + "-key",
                                                      schema_registry=schema_registry,
                                                      http_basic_auth=http_basic_auth)
        return key_schema, value_schema
