Apache Kafka Clients for Confluent Cloud
=====================================================

Clients:

- **Java** - based on the original Apache Kafka client API, but uses
Confluent's serializer e deserializar to handle records using Avro.

- **Go** - based on confluent-kafka-go, a lightweight wrapper around
[librdkafka](https://github.com/edenhill/librdkafka), a finely tuned C
client. It uses [goavro](github.com/linkedin/goavro) from LinkedIn to
serialize and deserialize records using Avro.

This README file is still in development so don't expect too much details here. The code is self-explanatory, so I would strongly recommend going straight there.
