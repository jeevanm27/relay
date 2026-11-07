// REST enqueue throughput.  Run through bench/run.mjs (which starts k6 in Docker), or by hand:
//   docker run --rm --network relaychaos_default -v ./bench/k6:/scripts -e BASE=http://relay-server:8080 \
//     -e VUS=32 -e DURATION=20s grafana/k6 run /scripts/enqueue.js
import http from "k6/http";
import { check } from "k6";
import { Counter } from "k6/metrics";

const created = new Counter("relay_created");
const BASE = __ENV.BASE || "http://localhost:8080";
const RUN = __ENV.RUN_ID || `${Date.now()}`;

export const options = {
  scenarios: {
    enqueue: {
      executor: "constant-vus",
      vus: Number(__ENV.VUS || 32),
      duration: __ENV.DURATION || "20s",
    },
  },
  summaryTrendStats: ["avg", "med", "p(95)", "p(99)", "max"],
  thresholds: { http_req_failed: ["rate<0.01"] },
};

export default function () {
  const key = `k6-${RUN}-${__VU}-${__ITER}`;
  const body = JSON.stringify({
    tenant: "bench",
    queue: __ENV.QUEUE || "k6",
    type: "noop",
    payload: { i: __ITER, pad: "xxxxxxxxxxxxxxxx" },
    idempotencyKey: key,
  });
  const res = http.post(`${BASE}/v1/jobs`, body, { headers: { "Content-Type": "application/json" } });
  if (check(res, { "201": (r) => r.status === 201 })) created.add(1);
}
