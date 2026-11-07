-- Chaos test fixtures (bench only; not part of the Flyway migrations).
DROP TABLE IF EXISTS side_effects, execution_log, acceptance_log, chaos_events;

-- The idempotent side effect: exactly one row per job, no matter how often the handler ran.
CREATE TABLE side_effects (
  job_id  UUID PRIMARY KEY,
  done_at TIMESTAMPTZ DEFAULT now()
);

-- One row per handler execution, written first thing by the handler. Duplicates (rows - N) prove at-least-once.
CREATE TABLE execution_log (
  id        BIGSERIAL PRIMARY KEY,
  job_id    UUID,
  attempt   INT,
  worker_id TEXT,
  at        TIMESTAMPTZ DEFAULT now()
);

-- One row per completion that the DATABASE ACCEPTED, with the attempt the worker believed it was completing.
-- Written by the worker right after complete() returned true (see ChaosListener). Used by invariant I5.
CREATE TABLE acceptance_log (
  id        BIGSERIAL PRIMARY KEY,
  job_id    UUID NOT NULL,
  attempt   INT  NOT NULL,
  worker_id TEXT,
  at        TIMESTAMPTZ DEFAULT now()
);
-- Fencing rejections and lease-lost interrupts, written durably so they survive kill -9.
CREATE TABLE chaos_events (
  id        BIGSERIAL PRIMARY KEY,
  kind      TEXT NOT NULL,
  job_id    UUID NOT NULL,
  attempt   INT  NOT NULL,
  worker_id TEXT,
  at        TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX execution_log_job_idx ON execution_log (job_id);
CREATE INDEX acceptance_log_job_idx ON acceptance_log (job_id);

-- Start every run from an empty queue.
TRUNCATE jobs, tenants CASCADE;
