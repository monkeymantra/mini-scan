package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/censys/scan-takehome/pkg/scanning/retrieval"
	"github.com/gocql/gocql"
	"log"
	"time"
)

func main() {
	clusterIp := flag.String("cassandra", "127.0.0.1", "Cluster IP")
	ip := flag.String("ip", "192.168.1.1", "IP to query")
	port := flag.Int("port", 80, "Port to query")
	service := flag.String("service", "http", "Service to query")
	flag.Parse()

	// Create a Cassandra cluster and session.
	cluster := gocql.NewCluster(*clusterIp)
	cluster.Keyspace = "scans"
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 10 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Failed to connect to Cassandra: %v", err)
	}
	defer session.Close()

	// Create our repository with a timeout (e.g., 5 seconds for each operation).
	repo := retrieval.NewScanRepository(session, 5*time.Second)

	// Create a context with a 10-second timeout for the overall operation.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use a fixed timestamp for deduplication/version_counter.
	ts := time.Now().UnixMilli()

	// Insert a new scan.
	data := "hello world!"
	version, err := repo.InsertScan(ctx, *ip, uint32(*port), *service, ts, data)
	if err != nil {
		log.Printf("InsertScan error: %v", err)
	} else {
		log.Printf("Inserted scan with version: %d", version)
	}

	// Attempt a duplicate insert (should return an error).
	_, err = repo.InsertScan(ctx, *ip, uint32(*port), *service, ts, data)
	if err != nil {
		log.Printf("Duplicate insert correctly not allowed: %v", err)
	} else {
		log.Printf("Unexpected: duplicate insert succeeded!")
	}

	// Retrieve the latest scan.
	latest, err := repo.GetLatestScan(ctx, *ip, uint32(*port), *service)
	if err != nil {
		log.Printf("GetLatestScan error: %v", err)
	} else {
		fmt.Printf("Latest scan: %+v\n", latest)
	}

}
