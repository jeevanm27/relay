-- Relay V1: schema (see docs/DESIGN.md for the reasoning behind each column and index).

CREATE TABLE tenants (
  id              TEXT PRIMARY KEY,
  max_concurrency INT  NOT NULL DEFAULT 50,
  weight          INT  NOT NULL DEFAULT 1,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE jobs (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id        TEXT NOT NULL REFERENCES tenants(id),
  queue            TEXT NOT NULL,
  type             TEXT NOT NULL,
  payload          JSONB NOT NULL DEFAULT '{}',
  state            TEXT NOT NULL DEFAULT 'PENDING'
                   CHECK (state IN ('PENDING','RUNNING','SUCCEEDED','DEAD')),
  priority         INT  NOT NULL DEFAULT 0,
  run_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  attempt          INT  NOT NULL DEFAULT 0,   -- fencing token, incremented on every claim, never decreases
  max_attempts     INT  NOT NULL DEFAULT 5,
  lease_owner      TEXT,
  lease_expires_at TIMESTAMPTZ,
  idempotency_key  TEXT,
  last_error       TEXT,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at       TIMESTAMPTZ,               -- first claim; used for pickup latency
  finished_at      TIMESTAMPTZ,
  CONSTRAINT jobs_attempt_nonneg CHECK (attempt >= 0),
  CONSTRAINT jobs_max_attempts_pos CHECK (max_attempts >= 1)
);

-- one logical job per (tenant, idempotency key)
CREATE UNIQUE INDEX jobs_idem_uq ON jobs (tenant_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

-- the claim path: only PENDING rows are indexed, so the index stays small
CREATE INDEX jobs_claim_idx ON jobs (queue, priority DESC, run_at, id)
  WHERE state = 'PENDING';

-- the reaper path
CREATE INDEX jobs_lease_idx ON jobs (lease_expires_at)
  WHERE state = 'RUNNING';
