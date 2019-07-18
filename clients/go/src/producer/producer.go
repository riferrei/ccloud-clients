package main

import (
	"ccloud"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/linkedin/goavro"
	"gopkg.in/confluentinc/confluent-kafka-go.v0/kafka"
)

// Order data type
type Order struct {
	ID     string  `json:"id"`
	Date   int64   `json:"date"`
	Amount float64 `json:"amount"`
}

const schemaFile string = "../../../../resources/orders.avsc"

func main() {

	topic := "orders"
	schema, _ := ioutil.ReadFile(schemaFile)
	props := make(map[string]string)
	ccloud.LoadProperties(props)

	schemaRegistryClient := ccloud.CreateSchemaRegistryClient(
		props["schema.registry.url"], props["schema.registry.basic.auth.username"],
		props["schema.registry.basic.auth.password"])

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":       props["bootstrap.servers"],
		"broker.version.fallback": "0.10.0.0",
		"api.version.fallback.ms": 0,
		"sasl.mechanisms":         "PLAIN",
		"security.protocol":       "SASL_SSL",
		"sasl.username":           props["sasl.username"],
		"sasl.password":           props["sasl.password"]})
	if err != nil {
		panic(fmt.Sprintf("Failed to create producer: %s", err))
	}

	for {

		// Create key and value
		key, _ := uuid.NewUUID()
		newOrder := Order{
			ID:     key.String(),
			Date:   time.Now().UnixNano(),
			Amount: float64(rand.Intn(1000))}
		value, _ := json.Marshal(newOrder)

		// Serialize the record value using Avro
		avroCodec, _ := goavro.NewCodec(string(schema))
		schemaID, err := schemaRegistryClient.CreateSubject(topic, avroCodec)
		if err != nil {
			panic(fmt.Sprintf("Error creating the subject in Schema Registry: %s", err))
		}
		schemaIDBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(schemaIDBytes, uint32(schemaID))
		native, _, _ := avroCodec.NativeFromTextual(value)
		valueBytes, _ := avroCodec.BinaryFromNative(nil, native)
		var recordValue []byte
		recordValue = append(recordValue, byte(0))
		recordValue = append(recordValue, schemaIDBytes...)
		recordValue = append(recordValue, valueBytes...)

		// Produce the record to the topic
		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic: &topic, Partition: kafka.PartitionAny},
			Key: []byte(key.String()), Value: recordValue}, nil)
		events := <-producer.Events()
		message := events.(*kafka.Message)
		if message.TopicPartition.Error == nil {
			fmt.Println("Order '" + key.String() + "' created successfully!")
		}

		// Sleep for one second...
		time.Sleep(1 * time.Second)

	}

}
