package main

import (
	"testing"
	"time"
)

func TestWorkerPublishesUpdatedVehicleData(t *testing.T) {
	vehicleChannel := make(chan VehicleData, 1)
	startData := VehicleData{VehicleID: 9, Latitude: 24.86, Longitude: 67.0, Speed: 15.0}

	go worker(vehicleChannel, startData)

	select {
	case got := <-vehicleChannel:
		if got.VehicleID != 9 {
			t.Fatalf("expected vehicle id 9, got %d", got.VehicleID)
		}
		if got.Timestamp.IsZero() {
			t.Fatal("expected worker to stamp the vehicle data")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for worker output")
	}
}
