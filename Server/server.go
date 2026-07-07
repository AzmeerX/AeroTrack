package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// Used as a buffer to hold vehicles until they are inserted in the db
var jobQueue = make(chan VehicleData, 1000)

// Mock database using a map for now
var database = make(map[int]VehicleData)
var databaseMutex sync.Mutex

// Global database pointer variable
var db *sql.DB

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
	dsn := "postgres://user:password@127.0.0.1:5433/telemetry?sslmode=disable"

	var err error

	// Open the persistent connection pool to the database
	db, err = sql.Open("pgx", dsn)
	if err != nil {
		log.Fatalf("Failed to initialize database pool: %v\n", err)
	}
	defer db.Close() // Ensure the pool closes when the server stops

	// Confirm the application can actively authenticating with the container
	if err := db.Ping(); err != nil {
		log.Fatalf("Unable to connect to Postgres container: %v\n", err)
	}
	fmt.Println("Connected to Postgres Container")

	http.HandleFunc("/telemetry", handleTelemetry)

	go func() {
		query := `
			INSERT INTO telemetry_history (vehicle_id, latitude, longitude, speed, timestamp) 
			VALUES ($1, $2, $3, $4, $5)
		`
		stmt, err := db.Prepare(query)
		if err != nil {
			log.Fatalf("Failed to prepare database insertion blueprint: %v", err)
		}
		defer stmt.Close()

		for vehicle := range jobQueue {
			// Step A: Keep in-memory map for immediate lookup operations
			databaseMutex.Lock()
			database[vehicle.VehicleID] = vehicle
			databaseMutex.Unlock()

			// Step B: Write the record permanently into the database table logs
			_, err := stmt.Exec(vehicle.VehicleID, vehicle.Latitude, vehicle.Longitude, vehicle.Speed, vehicle.Timestamp)
			if err != nil {
				fmt.Printf("Postgres Save Failure for Vehicle %d: %v\n", vehicle.VehicleID, err)
				continue
			}

			if vehicle.Speed > 80 {
				fmt.Printf("[ALERT] Vehicle %d is speeding! Speed: %.1f km/h\n", vehicle.VehicleID, vehicle.Speed)
			}
		}
	}()

	fmt.Println("Server running on :8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
