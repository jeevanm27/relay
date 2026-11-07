// Generates deploy/grafana/dashboards/relay.json.  Run: node deploy/grafana/gen-dashboard.js
const fs = require("fs");
const path = require("path");
const ds = { type: "prometheus", uid: "prometheus" };
let id = 0;
const panel = (title, x, y, w, h, targets, unit) => ({
  id: ++id, title, type: "timeseries", datasource: ds, gridPos: { x, y, w, h },
  fieldConfig: { defaults: { unit, custom: { lineWidth: 2, fillOpacity: 10 } }, overrides: [] },
  options: { legend: { displayMode: "list", placement: "bottom" }, tooltip: { mode: "multi" } },
  targets: targets.map(([expr, legend], i) => ({ refId: String.fromCharCode(65 + i), expr, legendFormat: legend, datasource: ds })),
});
const hq = (metric, p) => `histogram_quantile(${p}, sum by (le) (rate(${metric}_bucket[30s])))`;
const dash = {
  uid: "relay-overview", title: "Relay overview", schemaVersion: 39, version: 1, refresh: "5s",
  time: { from: "now-15m", to: "now" }, tags: ["relay"], timezone: "browser",
  panels: [
    panel("Queue depth (PENDING and due) and running", 0, 0, 8, 8,
      [["sum by (queue) (relay_queue_depth)", "pending {{queue}}"], ["sum by (queue) (relay_running_jobs)", "running {{queue}}"]], "short"),
    panel("Throughput (jobs finished / s)", 8, 0, 8, 8,
      [["sum by (outcome) (rate(relay_jobs_finished_total[30s]))", "{{outcome}}"]], "ops"),
    panel("Pickup latency p50 / p95 / p99", 16, 0, 8, 8,
      [[hq("relay_job_pickup_seconds", 0.5), "p50"], [hq("relay_job_pickup_seconds", 0.95), "p95"], [hq("relay_job_pickup_seconds", 0.99), "p99"]], "s"),
    panel("Failure rate (retried + dead share of finished)", 0, 8, 8, 8,
      [['sum(rate(relay_jobs_finished_total{outcome!="succeeded"}[30s])) / clamp_min(sum(rate(relay_jobs_finished_total[30s])), 0.0001)', "failure ratio"]], "percentunit"),
    panel("DLQ size", 8, 8, 8, 8, [["sum by (queue) (relay_dlq_size)", "{{queue}}"]], "short"),
    panel("Lease expirations and fencing rejections / s", 16, 8, 8, 8,
      [["sum(rate(relay_lease_expirations_total[30s]))", "lease expirations"], ["sum(rate(relay_fencing_rejections_total[30s]))", "fencing rejections"]], "ops"),
    panel("Handler duration p50 / p95 / p99", 0, 16, 12, 8,
      [[hq("relay_job_duration_seconds", 0.5), "p50"], [hq("relay_job_duration_seconds", 0.95), "p95"], [hq("relay_job_duration_seconds", 0.99), "p99"]], "s"),
    panel("Enqueue rate via REST (jobs / s)", 12, 16, 12, 8,
      [["sum by (queue) (rate(relay_jobs_enqueued_total[30s]))", "{{queue}}"]], "ops"),
  ],
};
const out = path.join(__dirname, "dashboards", "relay.json");
fs.mkdirSync(path.dirname(out), { recursive: true });
fs.writeFileSync(out, JSON.stringify(dash, null, 2));
console.log("wrote " + out + " with " + dash.panels.length + " panels");
