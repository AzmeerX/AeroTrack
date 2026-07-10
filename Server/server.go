package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/segmentio/kafka-go"
)

var kafkaWriter *kafka.Writer

type VehicleData struct {
	VehicleID int       `json:"vehicle_id"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	Speed     float64   `json:"speed"`
	Timestamp time.Time `json:"timestamp"`
}

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

	// Re-marshal the valid struct back into raw bytes for transmission to Kafka
	payload, err := json.Marshal(vehicle)
	if err != nil {
		http.Error(w, "Internal serialization failure", http.StatusInternalServerError)
		return
	}

	// Publish the payload directly into the Kafka cluster topic
	err = kafkaWriter.WriteMessages(r.Context(), kafka.Message{
		Value: payload,
	})
	if err != nil {
		fmt.Printf("Failed to publish event to Kafka cluster: %v\n", err)
		http.Error(w, "Broker unavailable", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func main() {
	// Initialize the writer, point to KRaft Kafka container port
	kafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP("127.0.0.1:9092"), // Changed to 127.0.0.1 for WSL stability
		Topic:    "telemetry_topic",
		Balancer: &kafka.LeastBytes{},
	}
	defer kafkaWriter.Close()

	http.HandleFunc("/telemetry", handleTelemetry)

	fmt.Println("Server running on :8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
