package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBuildVehicleDataUsesExpectedDefaults(t *testing.T) {
	data := buildVehicleData(3)

	if data.VehicleID != 3 {
		t.Fatalf("expected vehicle id 3, got %d", data.VehicleID)
	}
	if data.Speed != 5.0 {
		t.Fatalf("expected default speed 5.0, got %v", data.Speed)
	}
	if data.Timestamp.IsZero() {
		t.Fatal("expected timestamp to be set")
	}
}

func TestPostTelemetrySendsPayloadToConfiguredEndpoint(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST request, got %s", r.Method)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	payload := VehicleData{VehicleID: 3, Latitude: 24.86, Longitude: 67.0, Speed: 20.0}
	if err := postTelemetry(server.URL, payload); err != nil {
		t.Fatalf("postTelemetry returned error: %v", err)
	}

	if requests != 1 {
		t.Fatalf("expected 1 request, got %d", requests)
	}
}
