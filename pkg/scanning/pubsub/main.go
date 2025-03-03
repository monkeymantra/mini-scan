package main

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"fmt"
	"github.com/censys/scan-takehome/pkg/scanning/types"
	"log"
	"os"
	"time"
)

func deserializeScan(data []byte) (*types.Scan, error) {
	var scan types.Scan
	if err := json.Unmarshal(data, &scan); err != nil {
		return nil, err
	}
	return &scan, nil
}

// publishTestScan serializes a Scan struct to JSON and publishes it to the specified topic.
func publishTestScan(ctx context.Context, client *pubsub.Client, topicID string, scan types.Scan) error {
	// Marshal the scan object to JSON.
	data, err := json.Marshal(scan)
	if err != nil {
		return fmt.Errorf("failed to marshal scan: %w", err)
	}
	topic := client.Topic(topicID)
	result := topic.Publish(ctx, &pubsub.Message{
		Data: data,
	})
	// Wait for the result and get the message ID.
	id, err := result.Get(ctx)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}
	log.Printf("Published message with ID: %s", id)
	return nil
}

func main() {
	// Set the PUBSUB_EMULATOR_HOST so that the Pub/Sub client connects to the local emulator.
	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")

	projectID := "test-project"
	topicID := "scan-topic"
	subscriptionID := "scan-topic"

	// Create a context with a timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Create a Pub/Sub client.
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create Pub/Sub client: %v", err)
	}
	defer client.Close()

	// Create a test Scan object.
	testScan := types.Scan{
		Ip:          "192.168.1.1",
		Port:        80,
		Service:     "http",
		Timestamp:   time.Now().UnixMilli(),
		DataVersion: 0,
		Data:        `{"response_str": "hello world"}`,
	}

	// Publish the test Scan to the topic.
	if err := publishTestScan(ctx, client, topicID, testScan); err != nil {
		log.Fatalf("Error publishing test scan: %v", err)
	}

	// Get the subscription.
	sub := client.Subscription(subscriptionID)
	log.Printf("Starting subscription on %s", subscriptionID)

	// Start receiving messages.
	err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		// Deserialize the message into a Scan struct.
		scan, err := deserializeScan(msg.Data)
		if err != nil {
			log.Printf("Error deserializing scan from pubsub: %v", err)
		}
		log.Printf("Received message %s: %+v", msg.ID, scan)
		msg.Ack()
	})
	if err != nil {
		log.Fatalf("Error receiving messages: %v", err)
	}
}
