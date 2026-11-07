#!/usr/bin/env node
// Relay chaos test.  Usage:  node bench/chaos/chaos.mjs --n 100000 --workers 8     (or: make chaos N=1000000)
//
// What it does
//   1. starts postgres + relay-server (migrations, reaper) and loads N jobs with unique idempotency keys
//   2. runs W worker CONTAINERS (separate OS processes) that execute the chaos handler
//   3. every 5-10 s it kill -9s a random worker and restarts it
//   4. periodically it creates a REAL zombie: it injects slow jobs, freezes (SIGSTOP-equivalent) a worker that is
//      holding one until the lease expired AND another worker took the job over, then thaws the frozen worker
//   5. after the queue drains it checks invariants I1..I5 with SQL and exits non-zero on any failure
import path from "node:path";
import fs from "node:fs";
import { fileURLToPath } from "node:url";
import { Pg, docker, run, sleep, rand, randInt, parseArgs, waitFor, fmt, buildImages } from "../lib.mjs";

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, "../..");
const composeFile = path.join(here, "docker-compose.chaos.yml");

const opts = parseArgs(process.argv.slice(2), {
  n: 100000, workers: 8, lease: 6, killMin: 5, killMax: 10, pauseMin: 15, pauseMax: 30,
  slowMs: 4000, slowJobs: 3, concurrency: 16, timeoutMin: 240, build: true, keep: false, obs: false, durable: false,
});
if (opts.noBuild) opts.build = false;
// The chaos test kills WORKERS, never the database, so it runs with synchronous_commit=off by default: that setting can
// only lose the last few commits if PostgreSQL itself crashes (never corrupts), and it removes the ~5 ms fsync of this
// laptop VM from the loop, which makes a 1M-job run practical. Pass --durable to run with synchronous_commit=on.
process.env.PG_SYNC_COMMIT = opts.durable ? "on" : "off";

const PROJECT = "relaychaos";
const compose = (...a) => run("docker", ["compose", "-p", PROJECT, "-f", composeFile, ...a], { cwd: root });
const pg = new Pg(`${PROJECT}-postgres-1`);
const env = {
  CHAOS_LEASE: String(opts.lease), CHAOS_SLOW_MS: String(opts.slowMs), RELAY_CONCURRENCY: String(opts.concurrency),
};
Object.assign(process.env, env);

const stats = { kills: 0, pauses: 0, zombieWakes: 0, injectedSlow: 0 };
const log = (m) => console.log(`[${new Date().toISOString().slice(11, 19)}] ${m}`);
const state = { done: false };
const busy = new Set();

async function main() {
  const t0 = Date.now();
  for (const j of ["relay-server/target/relay-server-0.1.0-SNAPSHOT.jar", "relay-worker/target/relay-worker-0.1.0-SNAPSHOT.jar"]) {
    if (!fs.existsSync(path.join(root, j))) throw new Error(`${j} missing: run  mvn -B -DskipTests package  first`);
  }
  log(`chaos run: N=${fmt.int(opts.n)} workers=${opts.workers} lease=${opts.lease}s concurrency=${opts.concurrency} synchronous_commit=${process.env.PG_SYNC_COMMIT}`);
  await compose("down", "-v", "--remove-orphans");
  if (opts.build) await buildImages(root);
  const up = await compose("up", "-d", "--wait", "postgres", "relay-server");
  if (up.code !== 0) throw new Error("compose up failed:\n" + up.stderr);
  if (opts.obs) await compose("--profile", "obs", "up", "-d", "prometheus", "grafana");

  // --- setup
  await pg.exec((await fs.promises.readFile(path.join(here, "schema.sql"), "utf8")));
  log(`loading ${fmt.int(opts.n)} jobs ...`);
  const loadStart = Date.now();
  await pg.exec(`
    INSERT INTO tenants (id) VALUES ('chaos') ON CONFLICT DO NOTHING;
    INSERT INTO jobs (tenant_id, queue, type, payload, max_attempts, idempotency_key)
    SELECT 'chaos', 'chaos', 'chaos', jsonb_build_object('i', g), 100, 'chaos-' || g FROM generate_series(1, ${opts.n}) g;
    ANALYZE jobs;`);
  log(`loaded in ${fmt.secs(Date.now() - loadStart)}`);

  // --- workers
  const scale = await compose("up", "-d", "--scale", `worker=${opts.workers}`, "--no-recreate", "worker");
  if (scale.code !== 0) throw new Error("starting workers failed:\n" + scale.stderr);
  const workers = await listWorkers();
  if (workers.length !== opts.workers) throw new Error(`expected ${opts.workers} workers, found ${workers.length}`);
  log(`workers: ${workers.map((w) => w.name.replace(PROJECT + "-", "")).join(", ")}`);
  const runStart = Date.now();

  const tasks = [killer(workers), pauser(workers), progress(runStart)];
  const drained = await waitDrain(runStart);
  state.done = true;
  await Promise.allSettled(tasks);
  await restoreAll(workers);
  const wall = Date.now() - runStart;
  if (!drained) log("TIMEOUT: queue did not drain");
  await sleep(2000);

  // --- verify
  const total = opts.n + stats.injectedSlow;
  const rows = await pg.exec(fs.readFileSync(path.join(here, "verify.sql"), "utf8"), { expected_jobs: total });
  const checks = rows.map(([name, ok, detail]) => ({ name, ok: ok === "t", detail }));
  if (!drained) checks.unshift({ name: "queue drained before timeout", ok: false, detail: "timeout" });

  const execRows = Number(await pg.scalar("SELECT count(*) FROM execution_log"));
  const rejections = Number(await pg.scalar("SELECT count(*) FROM chaos_events WHERE kind = 'fencing_rejection'"));
  const leaseLost = Number(await pg.scalar("SELECT count(*) FROM chaos_events WHERE kind = 'lease_lost'"));
  const accepted = Number(await pg.scalar("SELECT count(*) FROM acceptance_log"));
  const maxAttempt = Number(await pg.scalar("SELECT coalesce(max(attempt),0) FROM jobs"));
  const leaseExpirations = await scrapeCounter("http://localhost:8081/actuator/prometheus", "relay_lease_expirations_total");

  const summary = {
    synchronousCommit: process.env.PG_SYNC_COMMIT, jobs: total, initialJobs: opts.n, injectedSlowJobs: stats.injectedSlow, workers: opts.workers, leaseSecs: opts.lease,
    kills: stats.kills, sigstopPauses: stats.pauses, zombieWakesWithTakeover: stats.zombieWakes,
    leaseExpirations, fencingRejections: rejections, leaseLostInterrupts: leaseLost,
    duplicateExecutions: execRows - total, executionLogRows: execRows, acceptedCompletionsAudited: accepted,
    maxAttemptOnAnyJob: maxAttempt, wallSeconds: +(wall / 1000).toFixed(1),
    throughputJobsPerSec: +(total / (wall / 1000)).toFixed(0), checks,
  };
  const pass = checks.every((c) => c.ok);
  printSummary(summary, pass);
  fs.mkdirSync(path.join(root, "bench/results"), { recursive: true });
  fs.writeFileSync(path.join(root, "bench/results", `chaos-${new Date().toISOString().replace(/[:.]/g, "-")}.json`),
    JSON.stringify({ ...summary, pass, finishedAt: new Date().toISOString() }, null, 2));

  if (!opts.keep) await compose("down", "-v", "--remove-orphans");
  console.log(`total elapsed ${fmt.secs(Date.now() - t0)}`);
  process.exit(pass ? 0 : 1);
}

async function listWorkers() {
  const r = await docker("ps", "-a", "--filter", `label=com.docker.compose.project=${PROJECT}`,
    "--filter", "label=com.docker.compose.service=worker", "--format", "{{.Names}}|{{.ID}}");
  return r.stdout.split(/\r?\n/).filter(Boolean).map((l) => {
    const [name, id] = l.split("|");
    return { name, hostname: id.slice(0, 12) }; // RELAY_WORKER_ID defaults to the container hostname = short id
  }).sort((a, b) => a.name.localeCompare(b.name));
}

async function killer(workers) {
  while (!state.done) {
    await sleepUnlessDone(rand(opts.killMin, opts.killMax) * 1000);
    if (state.done) break;
    const candidates = workers.filter((w) => !busy.has(w.name));
    if (!candidates.length) continue;
    const w = candidates[randInt(0, candidates.length - 1)];
    busy.add(w.name);
    const k = await docker("kill", "--signal", "KILL", w.name); // kill -9
    if (k.code === 0) { stats.kills++; log(`kill -9 ${short(w)} (#${stats.kills})`); }
    await sleep(rand(500, 3000));
    await docker("start", w.name);
    busy.delete(w.name);
  }
}

/** Creates real zombies: a frozen worker whose job was taken over by someone else, then thawed. */
async function pauser(workers) {
  while (!state.done) {
    await sleepUnlessDone(rand(opts.pauseMin, opts.pauseMax) * 1000);
    if (state.done) break;
    try {
      const keys = `${Date.now()}`;
      await pg.exec(`INSERT INTO jobs (tenant_id, queue, type, payload, priority, max_attempts, idempotency_key)
        SELECT 'chaos', 'chaos', 'chaos-slow', '{}', 100, 100, 'slow-${keys}-' || g FROM generate_series(1, ${opts.slowJobs}) g`);
      stats.injectedSlow += opts.slowJobs;

      // wait for some worker to be running one of the slow jobs
      let victim, job;
      await waitFor("a slow job to be running", async () => {
        const rows = await pg.exec(`SELECT id, lease_owner, attempt FROM jobs WHERE type = 'chaos-slow' AND state = 'RUNNING' ORDER BY random() LIMIT 5`);
        for (const [id, owner, attempt] of rows) {
          const w = workers.find((x) => x.hostname === owner && !busy.has(x.name));
          if (w) { victim = w; job = { id, attempt: Number(attempt) }; return true; }
        }
        return false;
      }, { timeoutMs: 30000, everyMs: 150 });

      busy.add(victim.name);
      const p = await docker("pause", victim.name); // cgroup freezer: every thread stops, like SIGSTOP
      if (p.code !== 0) { busy.delete(victim.name); continue; }
      stats.pauses++;
      log(`SIGSTOP-equivalent: froze ${short(victim)} holding slow job ${job.id.slice(0, 8)} attempt ${job.attempt} (#${stats.pauses})`);

      // keep it frozen until the lease expired and somebody else took the job over (this is what makes a zombie)
      let tookOver = false;
      try {
        await waitFor("takeover", async () => {
          const [row] = await pg.exec(`SELECT state, attempt FROM jobs WHERE id = '${job.id}'`);
          return row && Number(row[1]) > job.attempt;
        }, { timeoutMs: (opts.lease + 25) * 1000, everyMs: 100 });
        tookOver = true;
      } catch { /* thaw anyway */ }
      await docker("unpause", victim.name);
      busy.delete(victim.name);
      if (tookOver) { stats.zombieWakes++; log(`SIGCONT: ${short(victim)} is a zombie now (job ${job.id.slice(0, 8)} was taken over)`); }
    } catch (e) {
      log(`pauser: ${e.message.split("\n")[0]}`);
      for (const w of workers) { if (busy.has(w.name)) { await docker("unpause", w.name); busy.delete(w.name); } }
    }
  }
}

async function progress(runStart) {
  let last = 0, lastT = Date.now();
  while (!state.done) {
    await sleepUnlessDone(10000);
    try {
      const [[done, running, pending]] = await pg.exec(`SELECT count(*) FILTER (WHERE state='SUCCEEDED'), count(*) FILTER (WHERE state='RUNNING'), count(*) FILTER (WHERE state='PENDING') FROM jobs`);
      const now = Date.now();
      const rate = ((Number(done) - last) / ((now - lastT) / 1000)).toFixed(0);
      last = Number(done); lastT = now;
      log(`succeeded=${fmt.int(done)} running=${running} pending=${fmt.int(pending)} rate=${rate}/s  kills=${stats.kills} pauses=${stats.pauses} zombieWakes=${stats.zombieWakes} elapsed=${fmt.secs(now - runStart)}`);
    } catch (e) { log(`progress query failed: ${e.message.split("\n")[0]}`); }
  }
}

async function waitDrain(runStart) {
  const deadline = runStart + opts.timeoutMin * 60000;
  while (Date.now() < deadline) {
    try {
      const open = Number(await pg.scalar(`SELECT count(*) FROM jobs WHERE state NOT IN ('SUCCEEDED','DEAD')`));
      if (open === 0) return true;
    } catch { /* transient */ }
    await sleep(2000);
  }
  return false;
}

async function restoreAll(workers) {
  for (const w of workers) { await docker("unpause", w.name); await docker("start", w.name); }
}

async function scrapeCounter(url, name) {
  try {
    const text = await (await fetch(url)).text();
    const line = text.split("\n").find((l) => l.startsWith(name));
    return line ? Math.round(Number(line.split(" ").pop())) : null;
  } catch { return null; }
}

async function sleepUnlessDone(ms) {
  const end = Date.now() + ms;
  while (!state.done && Date.now() < end) await sleep(Math.min(250, end - Date.now()));
}
const short = (w) => w.name.replace(PROJECT + "-", "");

function printSummary(s, pass) {
  const line = "=".repeat(78);
  console.log(`\n${line}\nCHAOS SUMMARY\n${line}`);
  console.log(`jobs (N)                    : ${fmt.int(s.jobs)}  (${fmt.int(s.initialJobs)} + ${s.injectedSlowJobs} injected slow jobs)`);
  console.log(`workers / lease             : ${s.workers} containers / ${s.leaseSecs}s`);
  console.log(`kill -9                     : ${s.kills}`);
  console.log(`SIGSTOP pauses              : ${s.sigstopPauses}  (zombie wake-ups after takeover: ${s.zombieWakesWithTakeover})`);
  console.log(`lease expirations           : ${s.leaseExpirations ?? "n/a"}`);
  console.log(`fencing rejections          : ${s.fencingRejections}  (lease-lost interrupts: ${s.leaseLostInterrupts})`);
  console.log(`duplicate executions        : ${fmt.int(s.duplicateExecutions)}  (execution_log rows ${fmt.int(s.executionLogRows)} - N; expected under at-least-once)`);
  console.log(`highest attempt on any job  : ${s.maxAttemptOnAnyJob}`);
  console.log(`wall time                   : ${fmt.secs(s.wallSeconds * 1000)}  (${s.throughputJobsPerSec} jobs/s under chaos)`);
  console.log(line);
  for (const c of s.checks) console.log(`${c.ok ? "PASS" : "FAIL"}  ${c.name}\n        ${c.detail}`);
  console.log(`${line}\nRESULT: ${pass ? "PASS" : "FAIL"}\n${line}`);
}

main().catch(async (e) => {
  console.error("chaos run aborted:", e.message);
  state.done = true;
  process.exit(2);
});
