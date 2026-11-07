// Shared helpers for the chaos driver and the benchmark runner. Zero dependencies: Node 18+ and the docker CLI.
import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
export const rand = (min, max) => min + Math.random() * (max - min);
export const randInt = (min, max) => Math.floor(rand(min, max + 1));

/** Runs a command, resolves { code, stdout, stderr }. Never rejects on a non-zero exit. */
export function run(cmd, args, { input, cwd } = {}) {
  return new Promise((resolve) => {
    const p = spawn(cmd, args, { cwd, stdio: ["pipe", "pipe", "pipe"], windowsHide: true });
    let stdout = "", stderr = "";
    p.stdout.on("data", (d) => (stdout += d));
    p.stderr.on("data", (d) => (stderr += d));
    p.on("error", (e) => resolve({ code: -1, stdout, stderr: String(e) }));
    p.on("close", (code) => resolve({ code, stdout, stderr }));
    if (input !== undefined) p.stdin.end(input); else p.stdin.end();
  });
}

export function runSync(cmd, args, opts = {}) {
  const r = spawnSync(cmd, args, { encoding: "utf8", windowsHide: true, ...opts });
  return { code: r.status, stdout: r.stdout ?? "", stderr: r.stderr ?? "" };
}

export function parseArgs(argv, defaults) {
  const out = { ...defaults };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const key = a.slice(2).replace(/-([a-z])/g, (_, c) => c.toUpperCase());
    const next = argv[i + 1];
    if (next === undefined || next.startsWith("--")) out[key] = true;
    else { out[key] = typeof defaults[key] === "number" ? Number(next) : next; i++; }
  }
  return out;
}

/** psql against the postgres container of a compose project. Returns rows as arrays of strings. */
export class Pg {
  constructor(container) { this.container = container; }

  async exec(sql, vars = {}) {
    const args = ["exec", "-i", this.container, "psql", "-U", "relay", "-d", "relay", "-X", "-q", "-At", "-F", "|",
      "-v", "ON_ERROR_STOP=1"];
    for (const [k, v] of Object.entries(vars)) args.push("-v", `${k}=${v}`);
    const r = await run("docker", args, { input: sql });
    if (r.code !== 0) throw new Error(`psql failed (${r.code}): ${r.stderr.trim() || r.stdout.trim()}\nSQL: ${sql.slice(0, 300)}`);
    return r.stdout.split(/\r?\n/).filter((l) => l.length).map((l) => l.split("|"));
  }

  async scalar(sql, vars) {
    const rows = await this.exec(sql, vars);
    return rows.length ? rows[0][0] : null;
  }
}

export const docker = (...args) => run("docker", args);

/**
 * Builds relay/relay-server:dev and relay/relay-worker:dev from a staging copy (Dockerfile + jar only) in the OS temp
 * dir. A small context is faster than sending the whole repo, and it also works when the repo lives in a folder that
 * Docker cannot read directly (for example OneDrive "files on demand" placeholders).
 * Requires `mvn -B -DskipTests package` to have run.
 */
export async function buildImages(root) {
  const stage = path.join(os.tmpdir(), "relay-image-build");
  fs.rmSync(stage, { recursive: true, force: true });
  for (const m of ["relay-server", "relay-worker"]) {
    const jar = path.join(root, m, "target", `${m}-0.1.0-SNAPSHOT.jar`);
    if (!fs.existsSync(jar)) throw new Error(`${jar} missing: run  mvn -B -DskipTests package  first`);
    const ctx = path.join(stage, m);
    fs.mkdirSync(path.join(ctx, m, "target"), { recursive: true });
    fs.copyFileSync(path.join(root, m, "Dockerfile"), path.join(ctx, "Dockerfile"));
    fs.copyFileSync(jar, path.join(ctx, m, "target", `${m}-0.1.0-SNAPSHOT.jar`));
    const r = await run("docker", ["build", "-q", "-t", `relay/${m}:dev`, ctx]);
    if (r.code !== 0) throw new Error(`docker build of ${m} failed:\n${r.stderr}`);
  }
}

export async function waitFor(what, fn, { timeoutMs = 120000, everyMs = 1000 } = {}) {
  const deadline = Date.now() + timeoutMs;
  let last;
  while (Date.now() < deadline) {
    try { const v = await fn(); if (v) return v; } catch (e) { last = e; }
    await sleep(everyMs);
  }
  throw new Error(`timed out waiting for ${what}${last ? `: ${last.message}` : ""}`);
}

export const fmt = {
  secs: (ms) => (ms / 1000).toFixed(1) + "s",
  int: (n) => Number(n).toLocaleString("en-US"),
};
