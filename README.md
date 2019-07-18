Apache Kafka Clients for Confluent Cloud<sup>TM</sup>
=====================================================

Clients:

- **Java** - based on the original Apache Kafka client API, but uses
Confluent's serializer e deserializar to handle records using Avro.

- **Go** - based on [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go), a lightweight wrapper around [librdkafka](https://github.com/edenhill/librdkafka), a finely tuned C
client. It uses [goavro](https://github.com/linkedin/goavro) from LinkedIn to serialize and deserialize records using Avro.

This README file is still in development so don't expect too many details here. The code is self-explanatory, so I would strongly recommend going straight there.
