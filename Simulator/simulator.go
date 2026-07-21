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

func buildVehicleData(index int) VehicleData {
	return VehicleData{
		VehicleID: index,
		Latitude:  24.86 + (rand.Float64() * 0.05),
		Longitude: 67.00 + (rand.Float64() * 0.05),
		Speed:     5.0,
		Timestamp: time.Now(),
	}
}

func postTelemetry(url string, data VehicleData) error {
	fmt.Println("Sending data for Vehicle:", data.VehicleID)

	jsonData, err := json.Marshal(data)
	if err != nil {
		fmt.Println("Error marshalling JSON:", err)
		return err
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		fmt.Println("Error sending POST:", err)
		return err
	}
	defer resp.Body.Close()

	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func startVehicleWorkers(vehicleChannel chan<- VehicleData, totalVehicles int, vehicleFactory func(int) VehicleData) {
	for i := 0; i < totalVehicles; i++ {
		startData := vehicleFactory(i)
		go worker(vehicleChannel, startData)
	}
}

func dispatchTelemetry(url string, vehicleChannel <-chan VehicleData, sender func(string, VehicleData) error) {
	for data := range vehicleChannel {
		go func(d VehicleData) {
			_ = sender(url, d)
		}(data)
	}
}

func main() {
	const url = "http://52.140.116.154:8080/telemetry"
	const totalVehicles = 100

	vehicleChannel := make(chan VehicleData, totalVehicles)
	startVehicleWorkers(vehicleChannel, totalVehicles, buildVehicleData)
	dispatchTelemetry(url, vehicleChannel, postTelemetry)
}
