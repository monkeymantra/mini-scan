package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/censys/scan-takehome/pkg/scanning/types"
	"log"
	"math/rand"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
)

var (
	services = []string{"HTTP", "SSH", "DNS"}
)

func main() {
	projectId := flag.String("project", "test-project", "GCP Project ID")
	topicId := flag.String("topic", "scan-topic", "GCP PubSub Topic ID")

	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")
	ctx := context.Background()

	client, err := pubsub.NewClient(ctx, *projectId)
	if err != nil {
		panic(err)
	}

	topic := client.Topic(*topicId)

	for range time.Tick(time.Second) {

		dataBytes, err := json.Marshal(&types.V2Data{ResponseStr: "Hello World"})
		if err != nil {
			log.Printf("Error marshalling data: %v", err)
		}
		// Modified the Scan struct to use a string instead of an interface{} as it was just a lot easier to
		// Use it in more places, and to pull the data back out for serving.
		scan := &types.Scan{
			Ip:          fmt.Sprintf("1.1.1.%d", rand.Intn(255)),
			Port:        uint32(rand.Intn(65535)),
			Service:     services[rand.Intn(len(services))],
			Timestamp:   time.Now().Unix(),
			DataVersion: types.V2,
			Data:        string(dataBytes),
		}

		serviceResp := fmt.Sprintf("service response: %d", rand.Intn(100))

		marshalledV1, err := json.Marshal(&types.V1Data{ResponseBytesUtf8: []byte(serviceResp)})
		if err != nil {
			log.Fatal("Unable to marshal v1 response")
		}
		marshalledV2, err := json.Marshal(&types.V2Data{ResponseStr: serviceResp})
		if err != nil {
			log.Fatal("Unable to marshal v2 response")
		}

		if rand.Intn(2) == 0 {
			scan.DataVersion = types.V1
			scan.Data = string(marshalledV1)
		} else {
			scan.DataVersion = types.V2
			scan.Data = string(marshalledV2)
		}

		encoded, err := json.Marshal(scan)
		if err != nil {
			panic(err)
		}

		_, err = topic.Publish(ctx, &pubsub.Message{Data: encoded}).Get(ctx)
		if err != nil {
			panic(err)
		} else {
			log.Printf("Published a message to topic: %s", *topicId)
		}
	}
}
