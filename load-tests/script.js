import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    clinical_stream: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '10s', target: 30 }, // Ramp up to 30 concurrent virtual users
        { duration: '30s', target: 30 }, // Sustain heavy load for 30 seconds
        { duration: '5s', target: 0 },   // Ramp down
      ],
    },
  },
};

const LOINC_VITALS = [
  { code: '8480-6', display: 'Systolic Blood Pressure', min: 95, max: 160, unit: 'mmHg' },
  { code: '8462-4', display: 'Diastolic Blood Pressure', min: 60, max: 100, unit: 'mmHg' },
  { code: '8867-4', display: 'Heart Rate', min: 55, max: 120, unit: '/min' },
  { code: '8310-5', display: 'Body Temperature', min: 36.1, max: 38.5, unit: 'Cel' },
  { code: '2708-6', display: 'Oxygen Saturation', min: 92, max: 100, unit: '%' },
];

export default function () {
  const vital = LOINC_VITALS[Math.floor(Math.random() * LOINC_VITALS.length)];
  const patientNum = Math.floor(Math.random() * 500) + 1;
  const roll = Math.random();

  let eventId;
  let value;

  if (roll < 0.80) {
    // 80% Valid unique event
    eventId = `evt-${__VU}-${__ITER}-${Date.now()}`;
    value = +(vital.min + Math.random() * (vital.max - vital.min)).toFixed(1);
  } else if (roll < 0.95) {
    // 15% Duplicate event (reuses a small pool of 20 event IDs)
    eventId = `evt-dup-${Math.floor(Math.random() * 20)}`;
    value = +(vital.min + Math.random() * (vital.max - vital.min)).toFixed(1);
  } else {
    // 5% Poison pill (impossible negative or extreme value -> goes to DLQ)
    eventId = `evt-poison-${__VU}-${__ITER}-${Date.now()}`;
    value = -99.9;
  }

  const payload = JSON.stringify({
    eventId: eventId,
    patientId: `pat-${patientNum}`,
    code: vital.code,
    display: vital.display,
    value: value,
    unit: vital.unit,
    effectiveTime: new Date().toISOString(),
  });

  const res = http.post('http://host.docker.internal:8081/api/v1/observations', payload, {
    headers: { 'Content-Type': 'application/json' },
  });

  check(res, {
    'status is 202': (r) => r.status === 202,
  });
}