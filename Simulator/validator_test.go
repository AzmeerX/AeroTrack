package main

import (
	"encoding/json"
	"testing"
)

func TestVehicleDataJSONRoundTrip(t *testing.T) {
	original := VehicleData{VehicleID: 7, Latitude: 24.86, Longitude: 67.0, Speed: 55.5}

	payload, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var decoded VehicleData
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if decoded.VehicleID != original.VehicleID || decoded.Speed != original.Speed {
		t.Fatalf("expected round-trip values to be preserved, got %#v", decoded)
	}
}
