package utils

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// ORDERS is the topic name
const ORDERS string = "orders"

// CreateTopic is a utility function that
// creates the topic if it doesn't exists.
func CreateTopic(props map[string]string) {

	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers":       props["bootstrap.servers"],
		"broker.version.fallback": "0.10.0.0",
		"api.version.fallback.ms": 0,
		"sasl.mechanisms":         "PLAIN",
		"security.protocol":       "SASL_SSL",
		"sasl.username":           props["sasl.username"],
		"sasl.password":           props["sasl.password"]})

	if err != nil {
		fmt.Printf("Failed to create Admin client: %s\n", err)
		os.Exit(1)
	}

	// Contexts are used to abort or limit the amount of time
	// the Admin call blocks waiting for a result.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create topics on cluster.
	// Set Admin options to wait for the operation to finish (or at most 60s)
	maxDuration, err := time.ParseDuration("60s")
	if err != nil {
		panic("time.ParseDuration(60s)")
	}

	results, err := adminClient.CreateTopics(ctx,
		[]kafka.TopicSpecification{{
			Topic:             ORDERS,
			NumPartitions:     4,
			ReplicationFactor: 3}},
		kafka.SetAdminOperationTimeout(maxDuration))

	if err != nil {
		fmt.Printf("Problem during the topic creation: %v\n", err)
		os.Exit(1)
	}

	// Check for specific topic errors
	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError &&
			result.Error.Code() != kafka.ErrTopicAlreadyExists {
			fmt.Printf("Topic creation failed for %s: %v",
				result.Topic, result.Error.String())
			os.Exit(1)
		}
	}

	adminClient.Close()

}

const propsFile string = "../../../../resources/ccloud.properties"

// LoadProperties loads the ccloud.properties file into the map passed
// as reference so the main apps can create producers and consumers
func LoadProperties(props map[string]string) {
	file, err := os.Open(propsFile)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if len(line) > 0 {
			if !strings.HasPrefix(line, "//") &&
				!strings.HasPrefix(line, "#") {
				if strings.HasPrefix(line, "sasl.jaas.config") {
					parts := strings.Split(line, "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required ")
					saslJaasConfig := strings.ReplaceAll(parts[1], ";", "")
					userPassHolder := strings.Split(saslJaasConfig, " ")
					userName := strings.Split(userPassHolder[0], "=")
					props["sasl.username"] = strings.ReplaceAll(userName[1], "\"", "")
					password := strings.Split(userPassHolder[1], "=")
					props["sasl.password"] = strings.ReplaceAll(password[1], "\"", "")
				} else if strings.HasPrefix(line, "schema.registry.basic.auth.user.info") {
					parts := strings.Split(line, "=")
					userPassHolder := strings.Split(parts[1], ":")
					props["schema.registry.basic.auth.username"] = strings.TrimSpace(userPassHolder[0])
					props["schema.registry.basic.auth.password"] = strings.TrimSpace(userPassHolder[1])
				} else {
					parts := strings.Split(line, "=")
					key := strings.TrimSpace(parts[0])
					value := strings.TrimSpace(parts[1])
					props[key] = value
				}
			}
		}
	}

}
