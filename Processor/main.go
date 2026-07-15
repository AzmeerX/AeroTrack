package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	// Import package to work with dotenv
	"github.com/joho/godotenv"
	"google.golang.org/genai"

	// Import Kafka package
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	// Import PostgreSQL driver
	_ "github.com/jackc/pgx/v5/stdlib"
)

// VehicleData stores the real-time metrics of a vehicle
type VehicleData struct {
	VehicleID int       `json:"vehicle_id"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	Speed     float64   `json:"speed"`
	Timestamp time.Time `json:"timestamp"`
}

var db *sql.DB

func main() {
	// Load environment variables
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found")
	}

	apiKey := os.Getenv("GEMINI_API_KEY")
	if apiKey == "" {
		log.Fatal("GEMINI_API_KEY is not set")
	}

	// Initialize Gemini Client
	ctx := context.Background()
	aiClient, aiErr := genai.NewClient(ctx, &genai.ClientConfig{APIKey: apiKey})
	if aiErr != nil {
		log.Fatalf("Failed to initialize Gemini Client: %v", aiErr)
	}

	// Use the current models API for content generation
	aiModel := aiClient.Models

	// Establish the persistent database connection pool
	dsn := "postgres://user:password@127.0.0.1:5433/telemetry?sslmode=disable"
	var err error
	db, err = sql.Open("pgx", dsn)
	if err != nil {
		log.Fatalf("Failed to initialize db pool: %v\n", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Unable to connect to Postgres container: %v\n", err)
	}
	fmt.Println("Processor connected to Postgres container")

	// Configure the Kafka Consumer Reader
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"127.0.0.1:9092"},
		Topic:    "telemetry_topic",
		GroupID:  "telemetry-processor-group", // Configure the Kafka Reader to join a scalable Consumer Group
		MinBytes: 1,                           // 1 byte to instantly consume low-frequency individual pings
		MaxBytes: 10e6,                        // 10MB
	})
	defer kafkaReader.Close()
	fmt.Println("Processor listening for events on 'telemetry_topic'")

	rdb := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379", // Use 127.0.0.1 for WSL
	})

	ctxStartup, cancelStartup := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelStartup()
	if err := rdb.Ping(ctxStartup).Err(); err != nil {
		log.Fatalf("Unable to connect to Redis container: %v\n", err)
	}
	fmt.Println("Processor connected to Redis")

	// Prepare the database insert statement early for performance
	query := `
		INSERT INTO telemetry_history (vehicle_id, latitude, longitude, speed, timestamp) 
		VALUES ($1, $2, $3, $4, $5)
	`
	stmt, err := db.Prepare(query)
	if err != nil {
		log.Fatalf("Failed to prepare database insertion: %v", err)
	}
	defer stmt.Close()

	// Continuous Consumption Loop
	for {
		msg, err := kafkaReader.ReadMessage(context.Background())
		if err != nil {
			fmt.Printf("Error reading from Kafka stream: %v\n", err)
			continue
		}

		var vehicle VehicleData
		if err := json.Unmarshal(msg.Value, &vehicle); err != nil {
			fmt.Printf("Error parsing JSON: %v\n", err)
			continue
		}

		// Save to PostgreSQL history table
		_, err = stmt.Exec(vehicle.VehicleID, vehicle.Latitude, vehicle.Longitude, vehicle.Speed, vehicle.Timestamp)
		if err != nil {
			fmt.Printf("Database Save Failure for Vehicle %d: %v\n", vehicle.VehicleID, err)
			continue
		}
		fmt.Printf("Processed and saved tracking data for Vehicle ID: %d\n", vehicle.VehicleID)

		// Create a short-lived context purely for the Redis cache operation
		redisCtx, redisCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)

		redisKey := fmt.Sprintf("vehicle:latest:%d", vehicle.VehicleID)

		// Cache optimization: Save the full message payload
		err = rdb.Set(redisCtx, redisKey, msg.Value, 0).Err()

		// Cancel the loop context to prevent memory/goroutine leaks
		redisCancel()

		if err != nil {
			fmt.Printf("Redis Cache Save Failed for Vehicle %d: %v\n", vehicle.VehicleID, err)
		}

		// AI TRIGGER: Anomaly Detection
		if vehicle.Speed > 80.0 {
			fmt.Printf("[ALERT] Vehicle %d is speeding (%.1f km/h). Triggering AI Analysis...\n", vehicle.VehicleID, vehicle.Speed)

			// Created goroutine so the AI call doesnt block consumer loop
			go func(v VehicleData) {
				prompt := fmt.Sprintf(
					"You are an AI fleet safety analyst for a ride-hailing company. "+
						"Vehicle %d is traveling at %.2f km/h at coordinates (Lat: %f, Lon: %f). "+
						"Write a strict, 2-sentence risk assessment alert for the fleet manager.",
					v.VehicleID, v.Speed, v.Latitude, v.Longitude,
				)

				aiCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				resp, err := aiModel.GenerateContent(aiCtx, "gemini-2.0-flash", []*genai.Content{
					genai.NewContentFromText(prompt, genai.RoleUser),
				}, nil)
				if err != nil {
					fmt.Printf("AI Analysis Failed for Vehicle %d: %v\n", v.VehicleID, err)
					return
				}

				if resp != nil && len(resp.Candidates) > 0 {
					fmt.Printf("\n[AI FLEET ANALYST - VEHICLE %d]\n%s\n\n", v.VehicleID, resp.Text())
				}
			}(vehicle)
		}
	}
}
