package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

// Global pointers for message broker and cache layers
var kafkaWriter *kafka.Writer
var rdb *redis.Client

type VehicleData struct {
	VehicleID int       `json:"vehicle_id"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	Speed     float64   `json:"speed"`
	Timestamp time.Time `json:"timestamp"`
}

// POST /telemetry - Ingests data from vehicles and streams to Kafka
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

	payload, err := json.Marshal(vehicle)
	if err != nil {
		http.Error(w, "Internal serialization failure", http.StatusInternalServerError)
		return
	}

	err = kafkaWriter.WriteMessages(r.Context(), kafka.Message{
		Key:   []byte(strconv.Itoa(vehicle.VehicleID)), // Hash the ID to lock it to a partition
		Value: payload,
	})
	if err != nil {
		fmt.Printf("Failed to publish event to Kafka cluster: %v\n", err)
		http.Error(w, "Broker unavailable", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// GET /location?id=<vehicle_id> - Fetches live location instantly from Redis cache
func handleGetLocation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Incorrect Method", http.StatusMethodNotAllowed)
		return
	}

	// Extract the "id" parameter
	idStr := r.URL.Query().Get("id")
	if idStr == "" {
		http.Error(w, "Missing vehicle 'id' query parameter", http.StatusBadRequest)
		return
	}

	// Validate the ID parameter
	_, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "Invalid vehicle ID format; must be an integer", http.StatusBadRequest)
		return
	}

	// Construct the matching Redis look-up key
	redisKey := fmt.Sprintf("vehicle:latest:%s", idStr)

	// Create a resilient network timeout context for Redis read
	ctx, cancel := context.WithTimeout(r.Context(), 100*time.Millisecond)
	defer cancel()

	// Query Redis for the cached JSON payload
	val, err := rdb.Get(ctx, redisKey).Result()
	if err == redis.Nil {
		// Key does not exist in Redis
		http.Error(w, "Vehicle live location data not found in cache", http.StatusNotFound)
		return
	} else if err != nil {
		// General cache network failure
		fmt.Printf("Redis lookup error: %v\n", err)
		http.Error(w, "Cache storage unavailable", http.StatusInternalServerError)
		return
	}

	// Return the raw JSON payload straight from Redis to the HTTP client
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(val))
}

func main() {
	// Initialize the writer, point to KRaft Kafka container port
	kafkaWriter = &kafka.Writer{
		Addr:                   kafka.TCP("kafka:9092"),
		Topic:                  "telemetry_topic",
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true, // Force Kafka to build the topic on the first write
	}
	defer kafkaWriter.Close()

	// Initialize the Redis Client Connection Pool
	rdb = redis.NewClient(&redis.Options{
		Addr: "redis:6379",
	})

	// Check if Redis is reachable on application start
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Unable to reach Redis pool: %v\n", err)
	}
	fmt.Println("Server connected to Redis")

	// Register HTTP server route pathways
	http.HandleFunc("/location", handleGetLocation)
	http.HandleFunc("/telemetry", handleTelemetry)

	fmt.Println("Server running on :8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
