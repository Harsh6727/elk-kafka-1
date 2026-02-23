package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"
)

// Ticket represents a single metro event
type Ticket struct {
	Timestamp string `json:"timestamp"`
	Host      string `json:"host"`
	CardID    string `json:"card_id"`
	Station   string `json:"station"`
	Action    string `json:"action"` // "IN" or "OUT"
	Price     int    `json:"price"`  // Cost in Rupees
}

var stations = []string{"Central Plaza", "Tech Park", "Old City", "Airport", "Grand Central"}
var deviceTypes = []string{"01", "02", "03"}

func main() {
	// Initialize random seed ONCE
	rand.Seed(time.Now().UnixNano())

	// 1. Setup Logging to file
	f, err := os.OpenFile("metro.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()

	log.SetOutput(f)

	fmt.Println("🚇 Metro System Web Server Started...")
	fmt.Println(" -> Logging to metro.log")
	fmt.Println(" -> Printing to console")
	fmt.Println(" -> OPEN YOUR BROWSER TO: http://localhost:9090")
	fmt.Println("--------------------------------------------------")

	// 2. Setup the HTTP Route
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Set headers so the browser knows we are streaming raw text/JSON continuously
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		// We use a Flusher to force the browser to display the data immediately
		// instead of waiting for the page to finish loading.
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
			return
		}

		// 3. Infinite Loop to simulate traffic (Runs while the browser tab is open)
		for {
			// Check if the user closed the browser tab
			select {
			case <-r.Context().Done():
				return // Stop generating if the browser disconnects
			default:
			}

			event := generateRandomEvent()

			// Convert struct to JSON
			logEntry, err := json.Marshal(event)
			if err != nil {
				log.Printf("JSON marshal error: %v", err)
				continue
			}

			// Append a newline character
			logEntry = append(logEntry, '\n')

			// --- 1. Print to console ---
			fmt.Print(string(logEntry))

			// --- 2. Write to log file ---
			_, _ = f.Write(logEntry)

			// --- 3. Stream to Web Browser ---
			_, err = w.Write(logEntry)
			if err != nil {
				return // Browser probably closed
			}
			flusher.Flush() // Force the browser to render the new line instantly

			// Speed Control (Set to 100 milliseconds for fast logs)
			time.Sleep(500 * time.Millisecond)
		}
	})

	// Start the web server on port 9090
	err = http.ListenAndServe(":9090", nil)
	if err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}

func generateRandomEvent() Ticket {
	actions := []string{"IN", "OUT"}
	action := actions[rand.Intn(len(actions))]

	price := 0
	if action == "OUT" {
		price = rand.Intn(50) + 10 // Random price between 10 and 60
	}

	// Generate Host string: Prefix (101) + Device Type + Device ID
	devType := deviceTypes[rand.Intn(len(deviceTypes))]
	devID := fmt.Sprintf("%03d", rand.Intn(5)+1) // Generates 001 through 005
	hostStr := fmt.Sprintf("101%s%s", devType, devID)

	return Ticket{
		Timestamp: time.Now().Format(time.RFC3339),
		Host:      hostStr,
		CardID:    fmt.Sprintf("CARD-%d", rand.Intn(100)+1000), // e.g., CARD-1045
		Station:   stations[rand.Intn(len(stations))],
		Action:    action,
		Price:     price,
	}
}
