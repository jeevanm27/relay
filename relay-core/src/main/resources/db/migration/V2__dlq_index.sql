-- Relay V2: dead-letter queue listing and the relay_dlq_size gauge.
-- Without this, "state = 'DEAD'" would be a sequential scan over every job ever run.
CREATE INDEX jobs_dead_idx ON jobs (tenant_id, queue, finished_at DESC, id)
  WHERE state = 'DEAD';
