package main

import (
	"encoding/binary"
	"fmt"
	"time"
	"utils"

	"github.com/riferrei/srclient"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {

	props := make(map[string]string)
	utils.LoadProperties(props)
	utils.CreateTopic(props)

	schemaRegistryClient := srclient.CreateSchemaRegistryClient(props["schema.registry.url"])
	schemaRegistryClient.SetCredentials(
		props["schema.registry.basic.auth.username"],
		props["schema.registry.basic.auth.password"])

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":       props["bootstrap.servers"],
		"broker.version.fallback": "0.10.0.0",
		"api.version.fallback.ms": 0,
		"sasl.mechanisms":         props["sasl.mechanism"],
		"security.protocol":       props["security.protocol"],
		"sasl.username":           props["sasl.username"],
		"sasl.password":           props["sasl.password"],
		"session.timeout.ms":      6000,
		"group.id":                "golang-consumer",
		"auto.offset.reset":       "latest"})
	if err != nil {
		panic(fmt.Sprintf("Failed to create consumer: %s", err))
	} else {
		defer consumer.Close()
	}

	schemaRegistryClient.EnableCaching(true)
	consumer.SubscribeTopics([]string{utils.ORDERS}, nil)

	for {
		record, err := consumer.ReadMessage(100 * time.Millisecond)
		if err == nil {
			// Retrieve the schema id from the record value
			schemaID := binary.BigEndian.Uint32(record.Value[1:5])
			// Load the schema from Schema Registry and create
			// a codec from it. Use it later to deserialize the
			// the record value.
			codec, err := schemaRegistryClient.GetSchema(int(schemaID))
			if err != nil {
				panic(fmt.Sprintf("Error using Schema Registry: %s", err))
			}
			// Deserialize the record value using the codec
			native, _, _ := codec.NativeFromBinary(record.Value[5:])
			order, _ := codec.TextualFromNative(nil, native)
			// Print the record value
			fmt.Println(string(order))
		}
	}

}
