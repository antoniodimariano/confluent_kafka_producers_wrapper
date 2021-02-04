# Created by Antonio Di Mariano (antonio.dimariano@gmail.com) at 08/01/2021
from confluent_kafka_producers_wrapper.classes.SchemaRegistrySidecar import SchemaRegistryDriver
import sys
import os
import datetime
from datetime import datetime, timezone
import avro
import json
from pathlib import Path


def build_producer_configuration(**kwargs):
    """
    This method builds the configuration for initializing the AVRO Confluent Producer.
    It parses each entry in kwargs and add each key/value to the producer_conf dict by replacing any occurrence of "_" with "."

    :param kwargs:
    :return:
    """
    try:
        kwargs['bootstrap_servers'] = kwargs.get('brokers_uri')
        producer_conf = {}
        if kwargs.get('basic_auth_credentials_source'):
            producer_conf['schema.registry.basic.auth.credentials.source'] = kwargs.get('basic_auth_credentials_source')
        if kwargs.get('basic_auth_user_info'):
            producer_conf['schema.registry.basic.auth.user.info'] = kwargs.get('basic_auth_user_info')

        # removing what it's not needed as producers' config
        kwargs.pop('brokers_uri', '')
        kwargs.pop('service_name', '')
        kwargs.pop('topic', '')
        kwargs.pop('basic_auth_credentials_source', '')
        kwargs.pop('basic_auth_user_info', '')

        # translating all the keys replacing the _ with . as requested
        for entry in kwargs:
            if kwargs.get(entry):
                producer_conf[entry.replace('_', '.')] = kwargs.get(entry)
        if 'ssl.ca.location' not in producer_conf:
            # here we are sure that the default certificate will be used
            cacert_path = Path(__file__).parent / "../std_ssl_cert/cacert.pem"

            producer_conf['ssl.ca.location'] = cacert_path
        elif 'ssl.ca.location' in producer_conf:
            producer_conf['ssl.ca.location'] = os.path.abspath(producer_conf.get('ssl.ca.location'))
        if 'ssl.certificate.location' in producer_conf:
            producer_conf['ssl.certificate.location'] = os.path.abspath(producer_conf.get('ssl.certificate.location'))
        if 'ssl.key.location' in producer_conf:
            producer_conf['ssl.key.location'] = os.path.abspath(producer_conf.get('ssl.key.location'))

        if 'security.protocol' not in producer_conf:
            producer_conf['security.protocol'] = 'plaintext'
        if kwargs.get('debug'):
            producer_conf["debug"] = "consumer"
        if not kwargs.get('api_version_request', 1):
            # this is for brokers <= 0.10
            producer_conf['api.version.request'] = 'false'
            producer_conf['broker.version.fallback'] = '0.9.0.1'

        return producer_conf
    except Exception as error:
        sys.stderr.write(' An EXCEPTION %s buiding the producer configuration' % error)
        return 0


def init_producer_with_schema_registry(topic, brokers_configuration, store_and_load_schema):
    """
    This method creates and returns an instance of  AvroProducer
    :param kwargs:
    :return:
    """
    from confluent_kafka.avro import AvroProducer
    topic_schema = SchemaRegistryDriver().get_key_schema_and_value_schema(topic=topic,
                                                                          schema_registry=brokers_configuration.get(
                                                                              'schema_registry_url'),
                                                                          basic_auth_user_info=brokers_configuration.get(
                                                                              'basic_auth_user_info'),
                                                                          store_and_load_schema=store_and_load_schema)

    if topic_schema:
        producer_conf = build_producer_configuration(**brokers_configuration)

        return AvroProducer(producer_conf, default_key_schema=topic_schema.get('topic_key_schema'),
                            default_value_schema=topic_schema.get('topic_value_schema'))
    raise Exception('Error initializing the AVRO Producer %s\n' % brokers_configuration)


def init_producer(brokers_configuration):
    """
    This method creates and returns an instance of Producer
    :param kwargs:
    :return:
    """
    from confluent_kafka import Producer
    producer_conf = build_producer_configuration(**brokers_configuration)
    return Producer(producer_conf)


def load_brokers_configuration_from_env_variables():
    return {
        'brokers_uri': os.environ.get('brokers'),
        'schema_registry_url': os.environ.get('schema_registry'),
        'security_protocol': os.environ.get('security_protocol'),
        'ssl_ca_location': os.environ.get('ssl_ca_location'),
        'ssl_certificate_location': os.environ.get('ssl_certificate_location'),
        'ssl_key_location': os.environ.get('ssl_key_location'),
        'basic_auth_credentials_source': os.environ.get('basic_auth_credentials_source'),
        'sasl_mechanisms': os.environ.get('sasl_mechanisms'),
        'debug': os.environ.get('debug'),
        'api_version_request': os.environ.get('api_version_request')

    }


class Producer:

    def __init__(self, topic, brokers_configuration=0, store_and_load_schema=1):

        self.topic = topic
        self.schema_registry = 0
        if not brokers_configuration:
            brokers_configuration = load_brokers_configuration_from_env_variables()

        if brokers_configuration.get('schema_registry_url'):
            self.schema_registry = brokers_configuration.get('schema_registry_url')
            self.producer = init_producer_with_schema_registry(topic=topic,
                                                               brokers_configuration=brokers_configuration,
                                                               store_and_load_schema=store_and_load_schema)
        else:
            self.producer = init_producer(brokers_configuration=brokers_configuration)

    def delivery_callback(self, err, msg):
        if err:
            sys.stderr.write("Failed to deliver message: {}".format(err))
        else:
            now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            sys.stderr.write("[" + now + "] Message produced to {} - partition:[{}] - offset:{} \n"
                             .format(msg.topic(), msg.partition(), msg.offset()))

    def produce_message_with_schema_registry(self, **kwargs):
        """
        This method accepts a list of messages.
        The producer is now running asynchronous and flush() is only called at the end of the loop.

        Thanks to @Magnus Edenhil
        Calling flush() after each send is ok, but it effectively makes it a synchronous producer which
        has its problems: https://github.com/edenhill/librdkafka/wiki/FAQ#why-is-there-no-sync-produce-interface

        produce() is asynchronous, all it does is enqueue the message on an internal queue which is later (>= queue.buffering.max.ms)
        served by internal threads and sent to the broker (if a leader is available, else wait some more).

        https://github.com/confluentinc/confluent-kafka-python/issues/137

        value must be a list. A check for the type(value) is done so as to be sure to have a list.

         :param kwargs:
         :return:
         """
        callback_function = kwargs.get('delivery_callback', self.delivery_callback)
        value = kwargs.get('value')
        if type(value) is dict:
            list_of_messages = [value]
        elif type(value) is list:
            list_of_messages = kwargs.get('value', None)
        else:
            now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            sys.stderr.write(
                '[' + now + '] Unknown type for the given value (not a dict not a list)\n')
            return 0

        key = kwargs.get('key', None)
        for value in list_of_messages:
            try:
                self.producer.produce(topic=self.topic, value=value, key=key, callback=callback_function)
            except avro.io.AvroTypeException as error:
                sys.stderr.write(" Avro ERROR: %s \n".format(error))
                from confluent_kafka_producers_wrapper.helpers.files_operations import remove_topic_schema
                remove_topic_schema(topic=self.topic, schema_registry=self.schema_registry)
                return 0
            except BufferError as error:
                sys.stderr.write("%% Buffer full error {} Producing records to the topic {} \n"
                                 .format(error, self.topic))
                self.producer.poll(10)
                self.producer.produce(topic=self.topic, value=value, key=key, callback=callback_function)
            self.producer.poll(0)
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        sys.stderr.write(
            '[' + now + '] Waiting for delivering %d message(s) to %s \n' % (
                len(self.producer), self.topic))
        self.producer.flush()  # wait for any remaining delivery reports.
        return {"topic": self.topic, "sent": True}

    def produce_message_with_no_schema_registry(self, **kwargs):
        """
        This method accepts a list of messages.
        The producer is now running asynchronous and flush() is only called at the end of the loop.

        Thanks to @Magnus Edenhil we know that calling flush() after each send is ok, but it effectively makes it a synchronous producer which
        has its problems: https://github.com/edenhill/librdkafka/wiki/FAQ#why-is-there-no-sync-produce-interface

        produce() is asynchronous, all it does is enqueue the message on an internal queue which is later (>= queue.buffering.max.ms)
        served by internal threads and sent to the broker (if a leader is available, else wait some more).

        https://github.com/confluentinc/confluent-kafka-python/issues/137

        value must be a list. A check for the type(value) is done so as to be sure to have a list.

        :param kwargs:
        :return:
        """
        value = kwargs.get('value')
        if type(value) is dict:
            list_of_messages = [value]
        elif type(value) is list:
            list_of_messages = kwargs.get('value', None)
        else:
            return 0
        for message in list_of_messages:
            try:
                self.producer.produce(self.topic, value=json.dumps(message), callback=self.delivery_callback)
            except BufferError as e:
                sys.stderr.write('%% Local producer queue is full ' \
                                 '(%d messages awaiting delivery): try again\n' %
                                 len(self.producer))

            self.producer.poll(0)

        sys.stderr.write('%% Waiting for %d deliveries\n' % len(self.producer))
        self.producer.flush()
        return {"topic": self.topic, "sent": True}

    def produce_message(self, **kwargs):
        """
        This method acts as a proxy for the two functions listed below
        :param kwargs:
        :return:
        """
        if self.schema_registry:
            self.produce_message_with_schema_registry(**kwargs)
        else:
            self.produce_message_with_no_schema_registry(**kwargs)
