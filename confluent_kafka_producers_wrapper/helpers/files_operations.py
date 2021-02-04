# Created by Antonio Di Mariano (antonio.dimariano@gmail.com) at 29/01/2021
from pathlib import Path
import json
import logging
logger = logging.getLogger()


def load_schema_from_file(topic, schema_registry, filename=None):
    try:
        if filename is None:
            filename = "../schemas_cache/topics_schemas.json"
        configuration_path = Path(__file__).parent / filename
        if not configuration_path.exists():
            return 0
        topics_schemas = json.load(open(configuration_path))
        return topics_schemas.get(schema_registry).get(topic)
    except Exception as error:
        logger.debug('EXCEPTION %s occurred loading the latest schema for the topic %s  \n' % (
            error, topic))
        return 0


def store_schema(topic, key_schema, value_schema, schema_registry, filename=None):
    """

    :param topic:
    :param key_schema:
    :param value_schema:
    :return:
    """

    try:
        if filename is None:
            filename = "../schemas_cache/topics_schemas.json"
        configuration_path = Path(__file__).parent / filename

        if not configuration_path.exists():
            # if the topics_schemas.json does not exists, it will be created.
            with open(configuration_path, 'w') as outfile:
                json.dump({}, outfile)
        topics_schemas = json.load(open(configuration_path))

        if not topics_schemas:
            topics_schemas = {schema_registry: {topic: {'value': value_schema, 'key': key_schema}}}
        else:
            if schema_registry in topics_schemas:
                topics_schemas[schema_registry][topic] = {'value': value_schema, 'key': key_schema}
            else:
                topics_schemas[schema_registry] = {topic: {'value': value_schema, 'key': key_schema}}

        with open(configuration_path, 'w') as outfile:
            json.dump(topics_schemas, outfile)
        logger.debug('value/key schema stored for the topic %s %s  \n' % (
                topic, schema_registry))
        return topics_schemas
    except Exception as error:
        logger.error('EXCEPTION %s occurred storing the latest schema for the topic %s  \n' % (
            error, topic))
        return 0


def remove_topic_schema(topic, schema_registry, filename=None):
    try:
        if filename is None:
            filename = "../schemas_cache/topics_schemas.json"
        configuration_path = Path(__file__).parent / filename

        all_stored_schemas = json.load(open(configuration_path))
        stored_schema_for_schema_registry = all_stored_schemas.get(schema_registry)
        if topic in stored_schema_for_schema_registry:
            stored_schema_for_schema_registry.pop(topic, '')
            with open(configuration_path, 'w') as outfile:
                json.dump(all_stored_schemas, outfile)
            return all_stored_schemas
        else:
            return 0
    except Exception as error:
        logger.error('EXCEPTION %s occurred removing the latest schema for the topic %s  \n' % (
                error, topic))
        return 0
