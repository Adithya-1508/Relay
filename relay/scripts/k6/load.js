// k6 load test for Relay's job enqueue API.
//
// usage:
//   k6 run -e BASE_URL=http://localhost:8081 \
//          -e EMAIL=load@example.com \
//          -e PASSWORD=load-secret-pw \
//          -e WORKSPACE_SLUG=loadtest \
//          scripts/k6/load.js
//
// The script:
//   1. Registers a user/workspace (idempotent — ignores 409)
//   2. Logs in to get an access token
//   3. Creates a pipeline (idempotent — ignores 409)
//   4. Enqueues "noop" jobs in a load profile and reports p50/p95/p99
//
// Tune via CLI flags or env vars: -e VUS=50 -e DURATION=30s

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8081';
const EMAIL = __ENV.EMAIL || 'load@example.com';
const PASSWORD = __ENV.PASSWORD || 'load-secret-pw';
const WORKSPACE_NAME = __ENV.WORKSPACE_NAME || 'Loadtest';
const WORKSPACE_SLUG = __ENV.WORKSPACE_SLUG || 'loadtest';
const PIPELINE_SLUG = __ENV.PIPELINE_SLUG || 'load-noop';

const enqueueLatency = new Trend('relay_enqueue_latency_ms', true);
const enqueueOk = new Rate('relay_enqueue_success');

export const options = {
  vus: Number(__ENV.VUS || 25),
  duration: __ENV.DURATION || '30s',
  thresholds: {
    relay_enqueue_success: ['rate>0.99'],
    relay_enqueue_latency_ms: ['p(95)<200', 'p(99)<500'],
  },
};

// setup runs once before VUs spin up. It returns shared state that all VUs
// receive as their first arg.
export function setup() {
  // 1. Register (ignore 409 — duplicate is fine).
  const reg = http.post(`${BASE_URL}/v1/auth/register`, JSON.stringify({
    email: EMAIL,
    password: PASSWORD,
    workspace_name: WORKSPACE_NAME,
    workspace_slug: WORKSPACE_SLUG,
  }), { headers: { 'Content-Type': 'application/json' } });
  if (reg.status !== 201 && reg.status !== 409) {
    throw new Error(`register failed: ${reg.status} ${reg.body}`);
  }

  // 2. Login.
  const login = http.post(`${BASE_URL}/v1/auth/login`, JSON.stringify({
    email: EMAIL, password: PASSWORD,
  }), { headers: { 'Content-Type': 'application/json' } });
  check(login, { 'login 200': (r) => r.status === 200 });
  const body = login.json();
  const token = body.data.tokens.access_token;

  // 3. Create pipeline (ignore 409).
  const pipe = http.post(`${BASE_URL}/v1/pipelines`, JSON.stringify({
    name: 'Loadtest noop', slug: PIPELINE_SLUG,
  }), { headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` } });
  if (pipe.status !== 201 && pipe.status !== 409) {
    throw new Error(`pipeline create failed: ${pipe.status} ${pipe.body}`);
  }

  return { token };
}

export default function (data) {
  const headers = {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${data.token}`,
  };
  const payload = JSON.stringify({
    kind: 'noop',
    payload: {},
  });

  const t0 = Date.now();
  const res = http.post(`${BASE_URL}/v1/pipelines/${PIPELINE_SLUG}/jobs`, payload, { headers });
  const elapsed = Date.now() - t0;
  enqueueLatency.add(elapsed);
  enqueueOk.add(res.status === 201);

  check(res, { 'enqueue 201': (r) => r.status === 201 });

  // Pace each VU at ~5 RPS to avoid hammering rate limits. Tweak as needed.
  sleep(0.2);
}
