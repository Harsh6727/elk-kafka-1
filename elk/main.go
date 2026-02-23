// package main

// import (
// 	"bufio"
// 	"bytes"
// 	"context"
// 	"fmt"
// 	"log"
// 	"net/http"
// 	"os"
// 	"os/signal"
// 	"runtime"
// 	"sync"
// 	"syscall"
// 	"time"

// 	"github.com/elastic/go-elasticsearch/v8"
// 	"github.com/elastic/go-elasticsearch/v8/esutil"
// 	"github.com/segmentio/encoding/json" // High-performance JSON
// )

// // Ticket represents the enriched structure with sliced fields
// type Ticket struct {
// 	Timestamp  string `json:"timestamp"`
// 	Host       string `json:"host"`
// 	DeviceType string `json:"device_type"`
// 	DeviceID   string `json:"device_id"`
// 	DeviceName string `json:"device_name"`
// 	CardID     string `json:"card_id"`
// 	Station    string `json:"station"`
// 	Action     string `json:"action"`
// 	Price      int    `json:"price"`
// }

// // DeviceNameMap for friendly names
// var DeviceNameMap = map[string]string{
// 	"01": "Gateway_Alpha",
// 	"02": "Edge_Node_Beta",
// 	"03": "Sensor_Hub_Gamma",
// }

// // ticketPool reuses Ticket structs to reduce Garbage Collection pressure
// var ticketPool = sync.Pool{
// 	New: func() interface{} {
// 		return &Ticket{}
// 	},
// }

// func main() {
// 	// 1. Initialize High-Performance ES Client
// 	es, err := elasticsearch.NewDefaultClient()
// 	if err != nil {
// 		log.Fatalf("Error creating ES client: %s", err)
// 	}

// 	// 2. Setup Bulk Indexer (The "Crate Shipper")
// 	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
// 		Index:         "metro-logs",
// 		Client:        es,
// 		NumWorkers:    runtime.NumCPU(),
// 		FlushBytes:    5 * 1024 * 1024, // 5MB batches
// 		FlushInterval: 1 * time.Second,
// 	})
// 	if err != nil {
// 		log.Fatalf("Error creating indexer: %s", err)
// 	}

// 	// 3. Connect to Generator Stream (Logstash-style Input)
// 	resp, err := http.Get("http://localhost:9090")
// 	if err != nil {
// 		log.Fatalf("❌ Generator not found on 9090: %v", err)
// 	}
// 	defer resp.Body.Close()

// 	// 4. Graceful Shutdown Setup
// 	ctx, cancel := context.WithCancel(context.Background())
// 	sigChan := make(chan os.Signal, 1)
// 	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
// 	go func() {
// 		<-sigChan
// 		fmt.Println("\nGracefully shutting down...")
// 		cancel()
// 	}()

// 	fmt.Println("🚀 High-Speed Ingester Started...")
// 	fmt.Println(" - Using Segmentio/JSON for speed")
// 	fmt.Println(" - Using sync.Pool for memory efficiency")

// 	// 5. Main Processing Loop
// 	scanner := bufio.NewScanner(resp.Body)
// 	for scanner.Scan() {
// 		// Borrow a struct from the pool
// 		t := ticketPool.Get().(*Ticket)

// 		// Unmarshal into the borrowed struct
// 		if err := json.Unmarshal(scanner.Bytes(), t); err != nil {
// 			ticketPool.Put(t)
// 			continue
// 		}

// 		// --- HIGH PERFORMANCE TRANSFORMATION ---
// 		if len(t.Host) >= 8 {
// 			t.DeviceType = t.Host[3:5] // Zero-allocation slicing
// 			t.DeviceID = t.Host[5:8]   // Zero-allocation slicing

// 			// Map lookup O(1)
// 			if name, exists := DeviceNameMap[t.DeviceType]; exists {
// 				t.DeviceName = name
// 			} else {
// 				t.DeviceName = "Unknown_Device"
// 			}
// 		}

// 		// Marshal back to bytes using optimized encoder
// 		payload, _ := json.Marshal(t)

// 		// Add to Bulk Indexer
// 		err := bi.Add(ctx, esutil.BulkIndexerItem{
// 			Action: "index",
// 			Body:   bytes.NewReader(payload),
// 			OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
// 				// Return to pool for reuse
// 				ticketPool.Put(t)
// 			},
// 			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
// 				log.Printf("Indexing failure: %v", err)
// 				ticketPool.Put(t)
// 			},
// 		})
// 		if err != nil {
// 			log.Printf("Indexer Add Error: %v", err)
// 			ticketPool.Put(t)
// 		}

// 		if ctx.Err() != nil {
// 			break
// 		}
// 	}

// 	// 6. Final cleanup: Flush any remaining logs
// 	if err := bi.Close(context.Background()); err != nil {
// 		log.Printf("Error closing indexer: %s", err)
// 	}

// 	stats := bi.Stats()
// 	fmt.Printf("\n--- Final Results ---\n")
// 	fmt.Printf("Successfully Indexed: %d\n", stats.NumFlushed)
// 	fmt.Printf("Failed:               %d\n", stats.NumFailed)
// }

// ******************************************************************************************************************************************************

// package main

// import (
// 	"bufio"
// 	"bytes"
// 	"context"
// 	"fmt"
// 	"log"
// 	"net/http"
// 	"os"
// 	"os/signal"
// 	"runtime"
// 	"strings"
// 	"sync"
// 	"syscall"
// 	"time"

// 	"github.com/elastic/go-elasticsearch/v8"
// 	"github.com/elastic/go-elasticsearch/v8/esutil"
// 	"github.com/segmentio/encoding/json"
// )

// // Ticket represents the enriched structure with sliced fields
// type Ticket struct {
// 	Timestamp  string `json:"timestamp"`
// 	Host       string `json:"host"`
// 	DeviceType string `json:"device_type"`
// 	DeviceID   string `json:"device_id"`
// 	DeviceName string `json:"device_name"`
// 	CardID     string `json:"card_id"`
// 	Station    string `json:"station"`
// 	Action     string `json:"action"`
// 	Price      int    `json:"price"`
// }

// var DeviceNameMap = map[string]string{
// 	"01": "Gateway_Alpha",
// 	"02": "Edge_Node_Beta",
// 	"03": "Sensor_Hub_Gamma",
// }

// var ticketPool = sync.Pool{
// 	New: func() interface{} {
// 		return &Ticket{}
// 	},
// }

// func main() {
// 	// 1. Initialize High-Performance Cluster Client
// 	cfg := elasticsearch.Config{
// 		Addresses: []string{
// 			"http://localhost:9200", // Main entry point
// 		},
// 		// Discovery allows Go to find the other 2 nodes in your Docker cluster
// 		DiscoverNodesOnStart:  true,
// 		DiscoverNodesInterval: 5 * time.Minute,
// 	}

// 	es, err := elasticsearch.NewClient(cfg)
// 	if err != nil {
// 		log.Fatalf("Error creating ES client: %s", err)
// 	}

// 	// 2. Ensure Index Settings are optimized for millions of logs
// 	initIndex(es, "metro-logs")

// 	// 3. Setup Bulk Indexer (Optimized for Cluster)
// 	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
// 		Index:         "metro-logs",
// 		Client:        es,
// 		NumWorkers:    runtime.NumCPU(),
// 		FlushBytes:    10 * 1024 * 1024, // Increased to 10MB for cluster efficiency
// 		FlushInterval: 2 * time.Second,
// 	})
// 	if err != nil {
// 		log.Fatalf("Error creating indexer: %s", err)
// 	}

// 	// 4. Connect to Generator Stream
// 	resp, err := http.Get("http://localhost:9090")
// 	if err != nil {
// 		log.Fatalf("❌ Generator not found on 9090: %v", err)
// 	}
// 	defer resp.Body.Close()

// 	// 5. Graceful Shutdown Setup
// 	ctx, cancel := context.WithCancel(context.Background())
// 	sigChan := make(chan os.Signal, 1)
// 	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
// 	go func() {
// 		<-sigChan
// 		fmt.Println("\nGracefully shutting down...")
// 		cancel()
// 	}()

// 	fmt.Println("🚀 Cluster-Aware Ingester Started...")
// 	fmt.Println(" - Parallel Shard Writing: Active (3 Shards)")
// 	fmt.Println(" - Refresh Interval: 30s (High Throughput)")

// 	// 6. Main Processing Loop
// 	scanner := bufio.NewScanner(resp.Body)
// 	for scanner.Scan() {
// 		t := ticketPool.Get().(*Ticket)

// 		if err := json.Unmarshal(scanner.Bytes(), t); err != nil {
// 			ticketPool.Put(t)
// 			continue
// 		}

// 		if len(t.Host) >= 8 {
// 			t.DeviceType = t.Host[3:5]
// 			t.DeviceID = t.Host[5:8]
// 			if name, exists := DeviceNameMap[t.DeviceType]; exists {
// 				t.DeviceName = name
// 			} else {
// 				t.DeviceName = "Unknown_Device"
// 			}
// 		}

// 		payload, _ := json.Marshal(t)

// 		err := bi.Add(ctx, esutil.BulkIndexerItem{
// 			Action: "index",
// 			Body:   bytes.NewReader(payload),
// 			OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
// 				ticketPool.Put(t)
// 			},
// 			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
// 				log.Printf("Indexing failure: %v", err)
// 				ticketPool.Put(t)
// 			},
// 		})
// 		if err != nil {
// 			ticketPool.Put(t)
// 		}

// 		if ctx.Err() != nil {
// 			break
// 		}
// 	}

// 	// 7. Cleanup
// 	if err := bi.Close(context.Background()); err != nil {
// 		log.Printf("Error closing indexer: %s", err)
// 	}

// 	stats := bi.Stats()
// 	fmt.Printf("\n--- Final Results ---\n")
// 	fmt.Printf("Successfully Indexed: %d\n", stats.NumFlushed)
// 	fmt.Printf("Failed:               %d\n", stats.NumFailed)
// }

// // initIndex applies the PUT /metro-logs settings automatically
// func initIndex(es *elasticsearch.Client, indexName string) {
// 	// Check if index exists
// 	res, err := es.Indices.Exists([]string{indexName})
// 	if err != nil {
// 		log.Fatalf("Error checking index: %s", err)
// 	}

// 	if res.StatusCode == 404 {
// 		fmt.Printf("Creating index %s with optimized cluster settings...\n", indexName)

// 		// This is the Go implementation of the PUT /metro-logs JSON
// 		settings := `{
// 			"settings": {
// 				"index.number_of_shards": 3,
// 				"index.number_of_replicas": 1,
// 				"index.refresh_interval": "30s"
// 			}
// 		}`

// 		res, err := es.Indices.Create(
// 			indexName,
// 			es.Indices.Create.WithBody(strings.NewReader(settings)),
// 		)
// 		if err != nil || res.IsError() {
// 			log.Fatalf("Could not create index: %v", err)
// 		}
// 		fmt.Println("✅ Index created successfully.")
// 	} else {
// 		fmt.Println("ℹ️ Index already exists, skipping creation.")
// 	}
// }

package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/segmentio/encoding/json"
	"github.com/segmentio/kafka-go"
)

type Ticket struct {
	Timestamp  string `json:"timestamp"`
	Host       string `json:"host"`
	DeviceType string `json:"device_type"`
	DeviceID   string `json:"device_id"`
	DeviceName string `json:"device_name"`
	CardID     string `json:"card_id"`
	Station    string `json:"station"`
	Action     string `json:"action"`
	Price      int    `json:"price"`
}

var DeviceNameMap = map[string]string{
	"01": "Gateway_Alpha",
	"02": "Edge_Node_Beta",
	"03": "Sensor_Hub_Gamma",
}

var ticketPool = sync.Pool{
	New: func() interface{} { return &Ticket{} },
}

const (
	kafkaBrokers    = "localhost:9092"
	consumerGroupID = "metro-ingester-group"
	indexName       = "metro-logs"
	channelBuffer   = 10000 //50k
)

func main() {

	// ----------------------------------------------------
	// Elasticsearch Client
	// ----------------------------------------------------
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{
			"http://localhost:9200",
			"http://localhost:9201",
			"http://localhost:9202",
		},
		DiscoverNodesOnStart:  true,
		DiscoverNodesInterval: 5 * time.Minute,
	})
	if err != nil {
		log.Fatalf("ES error: %v", err)
	}

	initIndex(es, indexName)

	// ----------------------------------------------------
	// High-Throughput Bulk Indexer
	// ----------------------------------------------------
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:         indexName,
		Client:        es,
		NumWorkers:    runtime.NumCPU() * 2,
		FlushBytes:    10 * 1024 * 1024, // 50MB bulks
		FlushInterval: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("Bulk indexer error: %v", err)
	}

	// ----------------------------------------------------
	// Kafka Reader
	// ----------------------------------------------------
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBrokers},
		GroupID: consumerGroupID,
		GroupTopics: []string{
			"logs.central",
			"logs.ticketing",
			"logs.edge.tom",
			"logs.edge.gate",
		},
		MinBytes: 10e3,
		MaxBytes: 10e6,
		MaxWait:  250 * time.Millisecond,
	})
	defer reader.Close()

	ctx, cancel := context.WithCancel(context.Background())

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("Shutting down...")
		cancel()
	}()

	fmt.Println("🚀 High-Throughput Kafka → Elasticsearch Pipeline Started")

	// ----------------------------------------------------
	// Buffered Channel for Backpressure
	// ----------------------------------------------------
	messageChan := make(chan kafka.Message, channelBuffer)

	// ----------------------------------------------------
	// Kafka Consumer Goroutine
	// ----------------------------------------------------
	go func() {
		for {
			m, err := reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("Kafka read error: %v", err)
				continue
			}
			messageChan <- m
		}
	}()

	// ----------------------------------------------------
	// Worker Pool
	// ----------------------------------------------------
	workerCount := runtime.NumCPU() //* 2
	var wg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for msg := range messageChan {

				t := ticketPool.Get().(*Ticket)

				if err := json.Unmarshal(msg.Value, t); err != nil {
					ticketPool.Put(t)
					continue
				}

				// Enrichment
				if len(t.Host) >= 8 {
					t.DeviceType = t.Host[3:5]
					t.DeviceID = t.Host[5:8]
					if name, ok := DeviceNameMap[t.DeviceType]; ok {
						t.DeviceName = name
					} else {
						t.DeviceName = "Unknown_Device"
					}
				}

				payload, _ := json.Marshal(t)
				localTicket := t

				err := bi.Add(ctx, esutil.BulkIndexerItem{
					Action: "index",
					Body:   bytes.NewReader(payload),
					OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
						ticketPool.Put(localTicket)
					},
					OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
						ticketPool.Put(localTicket)
					},
				})

				if err != nil {
					ticketPool.Put(localTicket)
				}
			}
		}()
	}

	<-ctx.Done()
	close(messageChan)
	wg.Wait()

	if err := bi.Close(context.Background()); err != nil {
		log.Printf("Bulk close error: %v", err)
	}

	stats := bi.Stats()
	fmt.Printf("\nIndexed: %d\nFailed: %d\n", stats.NumFlushed, stats.NumFailed)
}

// ----------------------------------------------------
func initIndex(es *elasticsearch.Client, index string) {

	res, _ := es.Indices.Exists([]string{index})
	if res.StatusCode == 404 {
		//12 shards
		settings := `{
			"settings": {
				"index.number_of_shards": 6,   
				"index.number_of_replicas": 0,
				"index.refresh_interval": "-1"
			}
		}`

		es.Indices.Create(index,
			es.Indices.Create.WithBody(strings.NewReader(settings)),
		)
		fmt.Println("Index created for high ingest mode.")
	}
}
