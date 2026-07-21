package main

import (
	"testing"
	"time"
)

func TestDispatchTelemetrySendsAllIncomingVehicleData(t *testing.T) {
	vehicleChannel := make(chan VehicleData, 2)
	vehicleChannel <- VehicleData{VehicleID: 1}
	vehicleChannel <- VehicleData{VehicleID: 2}
	close(vehicleChannel)

	var sent []int
	completed := make(chan struct{}, 2)

	sender := func(url string, data VehicleData) error {
		sent = append(sent, data.VehicleID)
		completed <- struct{}{}
		return nil
	}

	dispatchTelemetry("http://example.com", vehicleChannel, sender)

	for i := 0; i < 2; i++ {
		select {
		case <-completed:
		case <-time.After(time.Second):
			t.Fatal("expected dispatch to send telemetry")
		}
	}

	if len(sent) != 2 {
		t.Fatalf("expected 2 dispatches, got %d", len(sent))
	}
}
