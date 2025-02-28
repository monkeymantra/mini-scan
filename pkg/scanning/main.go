package main

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/censys/scan-takehome/pkg/scanning/retrieval"
	"github.com/censys/scan-takehome/pkg/scanning/types"
	"github.com/gocql/gocql"
	"log"
	"os"
	"sync"
	"time"
)

// ------------------------------
// Pub/Sub Processing Helpers
// ------------------------------
// deserializeTempScan unmarshals the raw JSON message into a TempScan. TempScan is an unopinionated "I don't know what version I am"
// struct that's basically meant to be an intermediate class used to pull the data out of the messages with the Data field in
// a raw Json.
func deserializeTempScan(messageData []byte) (*types.TempScan, error) {
	var ts types.TempScan
	if err := json.Unmarshal(messageData, &ts); err != nil {
		return nil, err
	}
	return &ts, nil
}

// processSingleMessage processes a single Pub/Sub message. It deserializes the message
// into a TempScan, then decodes the Data field based on DataVersion into either V1Data or V2Data,
// then constructs a Scan instance and (for example purposes) prints it.
func processSingleMessage(ctx context.Context, repo retrieval.ScanRepositoryInterface, msg *pubsub.Message) error {
	// First, unmarshal into TempScan.
	tempScan, err := deserializeTempScan(msg.Data)
	if err != nil {
		return fmt.Errorf("error unmarshaling message %s: %v", msg.ID, err)
	}

	var decodedString string
	// Choose decoding based on DataVersion. I'm pretty new to Golang, so handling the unmarshalling
	// Wasn't going as well as I'd hoped it to. Either way, this is what I threw together in the interest of time.
	switch tempScan.DataVersion {
	case 1:
		var v1 map[string][]byte
		if err := json.Unmarshal(tempScan.Data, &v1); err != nil {
			return fmt.Errorf("error unmarshaling V1Data for message %s: %v", msg.ID, err)
		}
		decodedString = string(v1["response_bytes_utf8"])
	case 2:
		var v2 map[string]string
		if err := json.Unmarshal(tempScan.Data, &v2); err != nil {
			return fmt.Errorf("error unmarshaling V2Data for message %s: %v", msg.ID, err)
		}
		decodedString = v2["response_str"]
	default:
		return fmt.Errorf("unknown data_version %d for message %s", tempScan.DataVersion, msg.ID)
	}

	// Construct the final Scan.
	scan := types.Scan{
		Ip:          tempScan.Ip,
		Port:        tempScan.Port,
		Service:     tempScan.Service,
		Timestamp:   tempScan.Timestamp,
		DataVersion: tempScan.DataVersion,
		Data:        decodedString,
	}
	log.Printf("processing message %s from %s:%s with service %s", msg.ID, scan.Ip, scan.Port, scan.Service)
	_, err = repo.InsertScan(ctx, scan.Ip, scan.Port, scan.Service, scan.Timestamp, scan.Data)
	if err != nil {
		return fmt.Errorf("error inserting scan: %v", err)
	}
	// Acknowledge the message.
	msg.Ack()
	return nil
}

// processMessages starts worker goroutines to process messages from the channel.
func processMessages(ctx context.Context, repo retrieval.ScanRepositoryInterface, messages <-chan *pubsub.Message, wg *sync.WaitGroup) {
	for msg := range messages {
		wg.Add(1)
		go func(m *pubsub.Message) {
			defer wg.Done()
			if err := processSingleMessage(ctx, repo, m); err != nil {
				log.Printf("Processing error for message %s: %v", m.ID, err)
				m.Nack()
			} else {
				log.Printf("Successfully processed message %s", m.ID)
				m.Ack()
			}
		}(msg)
	}
}

// processPubSubMessages pulls messages from a Pub/Sub subscription and processes them in parallel.
func processPubSubMessages(ctx context.Context, repo retrieval.ScanRepositoryInterface, sub *pubsub.Subscription, workerCount int) error {
	// Create a buffered channel to hold incoming messages. I was wondering about doing some deduplication
	// here, but because there may be any number of workers, the deduplication needs to happen at the DB level.
	msgCh := make(chan *pubsub.Message, 100)
	var wg sync.WaitGroup

	// Start worker goroutines.
	for i := 0; i < workerCount; i++ {
		go processMessages(ctx, repo, msgCh, &wg)
	}
	err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		msgCh <- msg
	})
	close(msgCh)
	wg.Wait()

	return err
}

func main() {
	projectId := flag.String("project", "test-project", "GCP Project ID")
	numWorkers := flag.Int("workers", 1, "Number of workers")
	clusterIp := flag.String("cassandra", "127.0.0.1", "Cluster IP")
	subscriptionId := flag.String("subscription", "scan-sub", "Subscription ID")
	timeout := flag.Duration("timeout", 3*time.Second, "Timeout")

	flag.Parse()
	// Set up the environment so you can
	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")
	// Set up Cassandra session.
	cluster := gocql.NewCluster(*clusterIp)
	cluster.Keyspace = "scans"
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 10 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Failed to connect to Cassandra: %v", err)
	}
	defer session.Close()

	repo := retrieval.NewScanRepository(session, *timeout)

	// Set up Pub/Sub client.
	ctx := context.Background()

	pubsubClient, err := pubsub.NewClient(ctx, *projectId)
	if err != nil {
		log.Fatalf("Failed to create Pub/Sub client: %v", err)
	}
	defer pubsubClient.Close()
	sub := pubsubClient.Subscription(*subscriptionId)

	// Process messages using N concurrent workers.
	if err := processPubSubMessages(ctx, repo, sub, *numWorkers); err != nil {
		log.Printf("Error processing Pub/Sub messages: %v", err)
	}
}
