package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// Used as a buffer to hold vehicles until they are inserted in the db
var jobQueue = make(chan VehicleData, 1000)

// Mock database using a map for now
var database = make(map[int]VehicleData)
var databaseMutex sync.Mutex

// VehicleData stores the real time metrics of a vehicle
type VehicleData struct {
	VehicleID int       `json:"vehicle_id"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	Speed     float64   `json:"speed"`
	Timestamp time.Time `json:"timestamp"`
}

// POST/telemetry receives data and stores in database
func handleTelemetry(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Incorrect Method", http.StatusMethodNotAllowed)
		return
	}

	defer r.Body.Close()

	var vehicle VehicleData

	err := json.NewDecoder(r.Body).Decode(&vehicle)
	if err != nil {
		http.Error(w, "Invalid JSON data", http.StatusBadRequest)
		return
	}

	// handle the storing of data concurrently in a seperate goroutine
	jobQueue <- vehicle

	w.WriteHeader(http.StatusOK)
}

func main() {
	http.HandleFunc("/telemetry", handleTelemetry)

	go func() {
		for vehicle := range jobQueue {
			databaseMutex.Lock()
			database[vehicle.VehicleID] = vehicle
			databaseMutex.Unlock()

			if vehicle.Speed > 80 {
				fmt.Printf("⚠️ [ALERT] Vehicle %d is speeding! Speed: %.1f km/h\n", vehicle.VehicleID, vehicle.Speed)
			}
		}
	}()

	fmt.Println("Server running on :8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
