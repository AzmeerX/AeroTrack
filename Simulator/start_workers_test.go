package main

import (
	"testing"
	"time"
)

func TestStartVehicleWorkersStartsRequestedGoroutines(t *testing.T) {
	vehicleChannel := make(chan VehicleData, 2)
	started := 0

	startVehicleWorkers(vehicleChannel, 2, func(index int) VehicleData {
		started++
		return VehicleData{VehicleID: index, Speed: 5.0}
	})

	for i := 0; i < 2; i++ {
		select {
		case <-vehicleChannel:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for worker output")
		}
	}

	if started != 2 {
		t.Fatalf("expected 2 vehicles to be created, got %d", started)
	}
}
