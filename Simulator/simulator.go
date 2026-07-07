package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"time"
)

// VehicleData stores the real time metrics of a vehicle
type VehicleData struct {
	VehicleID int       `json:"vehicle_id"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	Speed     float64   `json:"speed"`
	Timestamp time.Time `json:"timestamp"`
}

// Simulate a moving vehicle, changing its metrics every second
func worker(vehicleChannel chan<- VehicleData, startData VehicleData) {
	// Physical speed limitations of vehicles 
	const maxSpeed = 120.0
	const minSpeed = 0.0
	const maxAcceleration = 5.0

	for {
		time.Sleep(time.Second * 1)

		startData.Timestamp = time.Now()

		// Simulate realistic changes in vehicle speed
		speedChange := (rand.Float64()*2 - 1) * maxAcceleration
		startData.Speed += speedChange

		// Caps the Speed of the Vehicle
		if startData.Speed > maxSpeed {
			startData.Speed = maxSpeed
		}
		if startData.Speed < minSpeed {
			startData.Speed = minSpeed
		}

		// Change the Latitude Longitude relative to the current speed of vehicle
		movementFactor := startData.Speed * 0.00002
		startData.Latitude += (rand.Float64() * movementFactor)
		startData.Longitude += (rand.Float64() * movementFactor)

		vehicleChannel <- startData
	}
}

func main() {
	const url = "http://localhost:8080/telemetry"
	const totalVehicles = 100

	vehicleChannel := make(chan VehicleData, totalVehicles)

	for i := range totalVehicles {
		startData := VehicleData{
			VehicleID: i,                               // might change later, for now give vehicle id's using loop counter
			Latitude:  24.86 + (rand.Float64() * 0.05), // Latitude according to Karachi's coordinates
			Longitude: 67.00 + (rand.Float64() * 0.05), // Longitude according to Karachi's coordinates
			Speed:     0.0,
			Timestamp: time.Now(),
		}

		go worker(vehicleChannel, startData)
	}

	// Fetch data from the channel and send it to POST/telemetry
	for data := range vehicleChannel {
		// Launch a goroutine for requesting the api concurrently
		go func(d VehicleData) {
			fmt.Println("Sending data for Vehicle:", d.VehicleID)

			jsonData, err := json.Marshal(d)
			if err != nil {
				fmt.Println("Error marshalling JSON:", err)
				return
			}

			resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
			if err != nil {
				fmt.Println("Error sending POST:", err)
				return
			}

			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}(data)

	}
}
