package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"time"
	"utils"

	"github.com/google/uuid"
	"github.com/riferrei/srclient"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Order data type
type Order struct {
	ID     string  `json:"id"`
	Date   int64   `json:"date"`
	Amount float64 `json:"amount"`
}

const schemaFile string = "../../../../resources/orders.avsc"

func main() {

	props := make(map[string]string)
	utils.LoadProperties(props)
	utils.CreateTopic(props)
	topic := utils.ORDERS

	schemaRegistryClient := srclient.CreateSchemaRegistryClient(props["schema.registry.url"])
	schemaRegistryClient.SetCredentials(
		props["schema.registry.basic.auth.username"],
		props["schema.registry.basic.auth.password"])

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": props["bootstrap.servers"],
		"sasl.mechanisms":   "PLAIN",
		"security.protocol": "SASL_SSL",
		"sasl.username":     props["sasl.username"],
		"sasl.password":     props["sasl.password"]})
	if err != nil {
		panic(fmt.Sprintf("Failed to create producer ---> %s", err))
	} else {
		defer producer.Close()
	}

	schema, err := schemaRegistryClient.GetLatestSchema(topic, false)
	if schema == nil {
		schemaBytes, _ := ioutil.ReadFile(schemaFile)
		schema, err = schemaRegistryClient.CreateSchema(topic, string(schemaBytes), false)
		if err != nil {
			panic(fmt.Sprintf("Error creating the schema ---> %s", err))
		}
	}

	for {

		// Create key and value
		key, _ := uuid.NewUUID()
		newOrder := Order{
			ID:     key.String(),
			Date:   time.Now().UnixNano(),
			Amount: float64(rand.Intn(1000))}
		value, _ := json.Marshal(newOrder)

		// Serialize the record value
		schemaIDBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(schemaIDBytes, uint32(schema.ID))
		native, _, _ := schema.Codec.NativeFromTextual(value)
		valueBytes, _ := schema.Codec.BinaryFromNative(nil, native)
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
