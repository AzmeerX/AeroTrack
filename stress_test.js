import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '30s', target: 50 },  // Ramp up: Scale from 0 to 50 concurrent vehicles
    { duration: '1m', target: 100 },  // Stress: Push up to 100 concurrent vehicles
    { duration: '30s', target: 0 },   // Cool down: Scale back down to 0
  ],
};

export default function () {
  const payload = JSON.stringify({
    vehicle_id: Math.floor(Math.random() * 1000) + 1,
    latitude: 24.8607 + (Math.random() - 0.5) * 0.1, 
    longitude: 67.0011 + (Math.random() - 0.5) * 0.1,
    speed: Math.random() * 120, // Simulating a mix of normal and speeding alerts
    timestamp: new Date().toISOString(),
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const res = http.post('http://127.0.0.1:8080/api/telemetry', payload, params);

  // Validate that the server is successfully returning 200 or 201 OK responses
  check(res, {
    'status is 200 or 201': (r) => r.status === 200 || r.status === 201,
    'response time < 200ms': (r) => r.timings.duration < 200,
  });

  // Small sleep to emulate actual hardware transmission frequency per vehicle
  sleep(0.1); 
}