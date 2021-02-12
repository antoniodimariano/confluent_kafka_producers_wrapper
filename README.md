
## Description:

This module provides a wrapper around the [confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python) 
to simplify the creation and usage of producers by hiding the configuration details. 
In particular, when it comes to using a schema registry, it provides a caching system that optimizes the number of requests sent to retrieve the schemas for topics. 
We do not apply changes to our schemas very often, so loading the schemas from the internal caching system improves the overall performances, avoiding networking delays and errors that might occur querying the Schema Registry multi times.
Moreover, to guarantee the cached information's consistency, if a schema validation error occurs, the cached schema is deleted so the new version of the schema can be retrieved from the Schema Registry and cached again.
This module comes handy with scenarios where we need to produce messages as fire-and-forget, even to different topics each time. 
For example, we might have a static method that we only call when we need to produce a message to a given Avro topic, passing just the topic and the message as parameters. In the static method, an instance of AvroProducer is created, and it will last just for the time needed to produce the message. Thanks to the internal caching system, every time we use our static method with the same topic, the schemas needed to initialize the AvroProducer will be loaded from a file instead of being requested from the Schema Registry. 

## How to use it:

### How to set the Environment variables

#### Plain-text Brokers

the bare minimum is 

```
brokers=mybroker:9090 mandatory
```

#### Schema Registry

If you brokers use a schema registry, then you have to specify this environment variable,

```
schema_registry= https://myschemaregistry:8081`
```

#### SSL identification

More information https://docs.confluent.io/3.0.0/kafka/ssl.html

If you broker uses SSL, like Aiven, you need to download these certificates files and copy them to a folder: 

* Access key 
* Access Certificate
* CA Certificate

Then, specify these environ variables





| ENV variable name        |                       value                      |
|--------------------------|:------------------------------------------------:|
| security_protocol         | SSL                                              |
| ssl\_ca_location          | the relative path to the CA certificate file     |
| ssl\_certificate_location | the relative path to the Access Certificate file |
| ssl\_key_location         | the relative path to the Access Key file         |



#### SASL Identification

More information https://docs.confluent.io/3.0.0/kafka/sasl.html
If your broker requires SASL authentication, like Confluent Cloud,  these are the ENVironment variables to include


| ENV variable name                                 |        value       |
|---------------------------------------------------|:------------------:|
| sasl_username                                     | YOUR USERNAME HERE |
| sasl_password                                     | YOUR PASSWORD HERE |
| schema\_registry\_basic\_auth\_user_info          | AUTH HERE          |
| schema\_registry\_basic\_auth\_credentials_source | USER_INFO          |
| sasl_mechanisms                                   | PLAIN              |
| security_protocol                                 | SASL_SSL           |




### Create producers

The only mandatory parameter to pass is the **topic**. Under the hood, the module will process the environ variables and configure the producer according. 
Other optional parameters are 

* brokers_configuration = a dictionary like 

```
{
    'brokers_uri': os.environ.get('brokers'),
    'schema_registry_url': os.environ.get('schema_registry'),
    'topic': 'tc-lab-data-sent-to-marketo',
    'security_protocol': os.environ.get('security_protocol'),
    'ssl_ca_location': os.environ.get('ssl_ca_location'),
    'ssl_certificate_location': os.environ.get('ssl_certificate_location'),
    'ssl_key_location': os.environ.get('ssl_key_location'),
    'basic_auth_credentials_source': os.environ.get('basic_auth_credentials_source'),
    'sasl_mechanisms': os.environ.get('sasl_mechanisms'),
    'debug': os.environ.get('debug'),
    'api_version_request': os.environ.get('api_version_request')

}
```
* store\_and_load\_schema = boolean. 

By default, the schemas are stored internally and loaded each time the same topic is passed as parameter. You can disable this behaviour by setting `store_and_load_schema=0`


#### Produce a message to an Avro topic


```
# Produce a  message to an Avro topic
from confluent_kafka_producers_wrapper.producer import Producer
producer = Producer(topic='mytopic')
message_to_send = {    
                       'timestamp': '2021-01-27T09:17:02+00:00',
                       'field1': 'test',
                       'customer_email': 'peterparker@spiderman.com',
                       'token': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUz4'
                    }

producer.produce_message(value=message_to_send, key={"my_key": 'my_key'})


```


```
#Example with a static method 

def my_fire_and_forget_producer(topic,message_value_payload, message_key_payload)
    producer = Producer(topic=topic)
    return producer.produce_message(value=message_value_payload,key= message_key_payload)
    
```

Every time I call the `my_fire_and_forget_producer` method with the same topic schemas will be loaded from the internal cache instead of being retrived from the Schema Registry.

#### Produce a  message to a Kafka topic:

```
# Produce a message to a Kafka topic
from confluent_kafka_producers_wrapper.producer import Producer
producer = Producer(topic='mytopic')
message_to_send = {    
                       'timestamp': '2021-01-27T09:17:02+00:00',
                       'field1': 'test',
                       'customer_email': 'peterparker@spiderman.com',
                       'token': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUz4'
                    }

producer.produce_message(value=message_to_send)


```




## Background consideration:

Assume to create an instance of a producer using the `confluent_kafka.avro` AvroProducer package from the confluent-kafka-python

As required, to create a producer, we need to provide schemas. 
Schemas can either be stored as .avsc files and load using the  `avro.load` method, 

```
from confluent_kafka import avro 
from confluent_kafka.avro import AvroProducer

value_schema = avro.load('ValueSchema.avsc')
key_schema = avro.load('KeySchema.avsc')
value = {"name": "Value"}
key = {"name": "Key"}

avroProducer = AvroProducer({'bootstrap.servers': 'mybroker,mybroker2', 'schema.registry.url': 'http://schem_registry_host:port'}, default_key_schema=key_schema, default_value_schema=value_schema)
avroProducer.produce(topic='my_topic', value=value, key=key)
```

or can be retrieved querying the Schema registry. In the latter, a few requests to the Schema Registry are needed to get the latest version of the topic's value and key schemas and then to retrieve their payload.

Anyhow, the `default_key_schema` and the `default_value_schema` values are mandatory fields for the AvroProducer class.

```
producer = AvroProducer(producer_conf, default_key_schema=topic_schema.get('topic_key_schema'),
                    default_value_schema=topic_schema.get('topic_value_schema'))
```

When an instance of AvroProducer is initialized, the Confluent Schema Registry component will retrieve and store the given topic's schemas in a local cache. The producer instance will keep using the local version of the schema for all its life unless a change will be applied to the schema that generates a validation error. 
Suppose another new instance of AvroProducer is created. In that case, the Schema Registry will be queried again to get the topic schemas, no matter if a previous instance has been created with the same topic.
There might be scenarios where we need to produce messages as a result of given actions on our service and to different topics. We want to have a  fire-and-forget behaviour and not keeping producing data. 
For example, we might have a static method that we only call when we need to produce a message to a given Avro topic, passing the topic and the message as parameters. In our static method, we create an instance of AvroProducer that will last for the time needed to produce the message. In this case, every time we create a new instance of AvroProducer, we need to retrieve from the Schema Registry the schemas for our topic. 
Most of the time, we do not apply changes to schemas very often. We could store the schemas instead of query for them, but we should do it for all the topics we want to use and update them if we apply schemas changes. 

This module provides a caching system to avoid sending multiple requests to the Schema Registry to retrieve the same schemas and use cached versions. It also provides a way to remove the cached version if a schema validation error occurs so the new version of the schema can be retrieved from the Schema Registry and cached.


## How the module works: 

![](kafka_producer_init.png)


 



 



