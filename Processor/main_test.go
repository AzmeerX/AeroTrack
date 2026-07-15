package main

import (
	"strings"
	"testing"
)

func TestBuildFallbackAssessmentContainsVehicleDetails(t *testing.T) {
	got := buildFallbackAssessment(12, 95.2, 24.86, 67.01)

	if !strings.Contains(got, "Vehicle 12") {
		t.Fatalf("expected assessment to mention vehicle id, got %q", got)
	}
	if !strings.Contains(got, "95.20 km/h") {
		t.Fatalf("expected assessment to include speed, got %q", got)
	}
	if !strings.Contains(got, "24.8600") {
		t.Fatalf("expected assessment to include latitude, got %q", got)
	}
	if strings.Count(got, ".") < 2 {
		t.Fatalf("expected assessment to be two sentences, got %q", got)
	}
}
