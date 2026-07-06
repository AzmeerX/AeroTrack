package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

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

	databaseMutex.Lock()
	database[vehicle.VehicleID] = vehicle
	fmt.Println(database) // Printing in critical section, latency bottleneck. Remove in production
	databaseMutex.Unlock()
	
	if vehicle.Speed > 80 {
		fmt.Printf("\n⚠️ [ALERT] Vehicle %d is speeding! Speed: %.1f", vehicle.VehicleID, vehicle.Speed)
	}

	w.WriteHeader(http.StatusOK)
}

func main() {
	http.HandleFunc("/telemetry", handleTelemetry)

	http.ListenAndServe(":8080", nil)
}
