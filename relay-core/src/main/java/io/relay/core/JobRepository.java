package io.relay.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

/**
 * All SQL of the queue lives here.
 *
 * <p>Rules enforced by this class (see docs/DESIGN.md):
 * <ul>
 *   <li>Every time value is PostgreSQL's {@code now()}. The JVM clock is never used for leases, run_at or backoff.</li>
 *   <li>Each public method is exactly one SQL statement in autocommit mode, i.e. one short transaction.
 *       No method holds a transaction open, and callers must never wrap a job handler in a transaction.</li>
 *   <li>{@code attempt} is the fencing token. heartbeat, complete and fail only match
 *       {@code (id, attempt, state = 'RUNNING')}. Nothing in this class ever lowers {@code attempt}.</li>
 * </ul>
 */
public class JobRepository {

    private static final int MAX_ERROR_CHARS = 4000;

    private final NamedParameterJdbcTemplate jdbc;
    private final RetryPolicy retryPolicy;

    public JobRepository(NamedParameterJdbcTemplate jdbc, RetryPolicy retryPolicy) {
        this.jdbc = jdbc;
        this.retryPolicy = retryPolicy;
    }

    public JobRepository(NamedParameterJdbcTemplate jdbc) {
        this(jdbc, RetryPolicy.DEFAULT);
    }

    public RetryPolicy retryPolicy() {
        return retryPolicy;
    }

    // ------------------------------------------------------------------ enqueue

    private static final String ENQUEUE_SQL = """
            WITH ensure_tenant AS (
              INSERT INTO tenants (id) VALUES (:tenant) ON CONFLICT (id) DO NOTHING
            )
            INSERT INTO jobs (tenant_id, queue, type, payload, priority, run_at, max_attempts, idempotency_key)
            VALUES (:tenant, :queue, :type, CAST(:payload AS jsonb), :priority,
                    COALESCE(CAST(:run_at AS timestamptz), now()), :max_attempts, :idem_key)
            ON CONFLICT (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
            RETURNING *
            """;

    /**
     * Idempotent enqueue. The tenant row is created on first use in the same statement (one round trip, atomic).
     * If the idempotency key already exists for this tenant the existing job is returned with {@code created=false}.
     */
    public EnqueueResult enqueue(EnqueueRequest req) {
        List<Job> inserted = jdbc.query(ENQUEUE_SQL, enqueueParams(req), JOB_MAPPER);
        if (!inserted.isEmpty()) {
            return new EnqueueResult(inserted.get(0), true);
        }
        // ON CONFLICT DO NOTHING returned no row: the key exists. Jobs are never deleted, so this lookup cannot miss.
        Job existing = findByIdempotencyKey(req.tenantId(), req.idempotencyKey())
                .orElseThrow(() -> new IllegalStateException("idempotency conflict but no existing job found"));
        return new EnqueueResult(existing, false);
    }

    /**
     * Bulk insert without idempotency keys handling beyond DO NOTHING; used by load generators.
     * Returns the number of rows actually inserted.
     */
    public int enqueueBatch(List<EnqueueRequest> requests) {
        if (requests.isEmpty()) {
            return 0;
        }
        for (String tenant : requests.stream().map(EnqueueRequest::tenantId).distinct().toList()) {
            jdbc.update("INSERT INTO tenants (id) VALUES (:t) ON CONFLICT (id) DO NOTHING", Map.of("t", tenant));
        }
        String sql = """
                INSERT INTO jobs (tenant_id, queue, type, payload, priority, run_at, max_attempts, idempotency_key)
                VALUES (:tenant, :queue, :type, CAST(:payload AS jsonb), :priority,
                        COALESCE(CAST(:run_at AS timestamptz), now()), :max_attempts, :idem_key)
                ON CONFLICT (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
                """;
        SqlParameterSource[] params = requests.stream().map(JobRepository::enqueueParams)
                .toArray(SqlParameterSource[]::new);
        int total = 0;
        for (int n : jdbc.batchUpdate(sql, params)) {
            // pgjdbc reports SUCCESS_NO_INFO (-2) when it rewrites batched inserts into multi-row statements
            total += n == java.sql.Statement.SUCCESS_NO_INFO ? 1 : n;
        }
        return total;
    }

    private static MapSqlParameterSource enqueueParams(EnqueueRequest r) {
        return new MapSqlParameterSource()
                .addValue("tenant", r.tenantId())
                .addValue("queue", r.queue())
                .addValue("type", r.type())
                .addValue("payload", r.payload())
                .addValue("priority", r.priority())
                .addValue("run_at", r.runAt() == null ? null : OffsetDateTime.ofInstant(r.runAt(), java.time.ZoneOffset.UTC),
                        Types.TIMESTAMP_WITH_TIMEZONE)
                .addValue("max_attempts", r.maxAttempts())
                .addValue("idem_key", r.idempotencyKey(), Types.VARCHAR);
    }

    // ------------------------------------------------------------------ read

    public Optional<Job> findById(UUID id) {
        return jdbc.query("SELECT * FROM jobs WHERE id = :id", Map.of("id", id), JOB_MAPPER).stream().findFirst();
    }

    public Optional<Job> findByIdempotencyKey(String tenantId, String key) {
        return jdbc.query("SELECT * FROM jobs WHERE tenant_id = :t AND idempotency_key = :k",
                Map.of("t", tenantId, "k", key), JOB_MAPPER).stream().findFirst();
    }

    // ------------------------------------------------------------------ claim

    /**
     * Single-statement batch claim. {@code FOR UPDATE SKIP LOCKED} makes concurrent claimers skip rows that another
     * transaction has already locked instead of waiting for them, so two claimers can never lock (and therefore never
     * update) the same row. Each claim increments {@code attempt}, which is the fencing token for this execution.
     */
    private static final String CLAIM_SQL = """
            WITH next AS MATERIALIZED (
              SELECT id FROM jobs
              WHERE queue = :queue AND state = 'PENDING' AND run_at <= now()
              ORDER BY priority DESC, run_at, id
              LIMIT :limit
              FOR UPDATE SKIP LOCKED
            )
            UPDATE jobs j SET
              state            = 'RUNNING',
              attempt          = j.attempt + 1,
              lease_owner      = :worker_id,
              lease_expires_at = now() + make_interval(secs => CAST(:lease_secs AS double precision)),
              started_at       = COALESCE(j.started_at, now()),
              updated_at       = now()
            FROM next WHERE j.id = next.id
            RETURNING j.*
            """;

    public List<Job> claim(String queue, String workerId, double leaseSecs, int limit) {
        if (limit <= 0) {
            return List.of();
        }
        if (leaseSecs <= 0) {
            throw new IllegalArgumentException("leaseSecs must be positive");
        }
        List<Job> claimed = jdbc.query(CLAIM_SQL, new MapSqlParameterSource()
                .addValue("queue", queue)
                .addValue("worker_id", workerId)
                .addValue("lease_secs", leaseSecs)
                .addValue("limit", limit), JOB_MAPPER);
        // UPDATE ... RETURNING has no defined order; hand jobs back in claim-priority order.
        List<Job> sorted = new ArrayList<>(claimed);
        sorted.sort(Comparator.comparingInt(Job::priority).reversed()
                .thenComparing(Job::runAt).thenComparing(Job::id));
        return sorted;
    }

    // ------------------------------------------------------------------ heartbeat / complete / fail

    /** Extends the lease. {@code false} means the lease was lost (stale attempt or no longer RUNNING): abandon the job. */
    public boolean heartbeat(UUID id, int attempt, double extendSecs) {
        return jdbc.update("""
                UPDATE jobs SET lease_expires_at = now() + make_interval(secs => CAST(:extend_secs AS double precision)),
                                updated_at = now()
                WHERE id = :id AND attempt = :attempt AND state = 'RUNNING'
                """, new MapSqlParameterSource()
                .addValue("extend_secs", extendSecs)
                .addValue("id", id)
                .addValue("attempt", attempt)) == 1;
    }

    /** Fenced completion. {@code false} means this caller is stale (a zombie) and must not assume its work counted. */
    public boolean complete(UUID id, int attempt) {
        return jdbc.update("""
                UPDATE jobs SET state = 'SUCCEEDED', finished_at = now(), updated_at = now(),
                                lease_owner = NULL, lease_expires_at = NULL
                WHERE id = :id AND attempt = :attempt AND state = 'RUNNING'
                """, Map.of("id", id, "attempt", attempt)) == 1;
    }

    /**
     * Fenced failure. Retries with full jitter ({@code random() * min(cap, base * 2^attempt)}) until
     * {@code attempt >= max_attempts}, then moves the job to DEAD. {@code retryable=false} goes straight to DEAD.
     * The exponent is clamped at 60 so {@code power(2, attempt)} cannot overflow to infinity for huge max_attempts.
     */
    private static final String FAIL_SQL = """
            UPDATE jobs SET
              state = CASE WHEN NOT :retryable OR attempt >= max_attempts THEN 'DEAD' ELSE 'PENDING' END,
              run_at = CASE WHEN NOT :retryable OR attempt >= max_attempts THEN run_at
                       ELSE now() + make_interval(secs => random() * LEAST(CAST(:cap_secs AS double precision),
                            CAST(:base_secs AS double precision) * power(2, LEAST(attempt, 60)))) END,
              finished_at = CASE WHEN NOT :retryable OR attempt >= max_attempts THEN now() END,
              last_error = :error, lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
            WHERE id = :id AND attempt = :attempt AND state = 'RUNNING'
            RETURNING state, extract(epoch FROM (run_at - now())) AS delay_secs
            """;

    public FailResult fail(UUID id, int attempt, String error, boolean retryable) {
        List<FailResult> rows = jdbc.query(FAIL_SQL, new MapSqlParameterSource()
                .addValue("id", id)
                .addValue("attempt", attempt)
                .addValue("error", truncate(error))
                .addValue("retryable", retryable)
                .addValue("cap_secs", retryPolicy.capSecs())
                .addValue("base_secs", retryPolicy.baseSecs()), (rs, i) -> {
                    if (JobState.valueOf(rs.getString("state")) == JobState.DEAD) {
                        return new FailResult(FailResult.Outcome.DEAD, 0);
                    }
                    return new FailResult(FailResult.Outcome.RETRIED, Math.max(0, rs.getDouble("delay_secs")));
                });
        return rows.isEmpty() ? FailResult.STALE : rows.get(0);
    }

    // ------------------------------------------------------------------ reaper

    private static final String REAP_SQL = """
            WITH expired AS MATERIALIZED (
              SELECT id FROM jobs
              WHERE state = 'RUNNING' AND lease_expires_at < now()
              ORDER BY lease_expires_at
              LIMIT :limit
              FOR UPDATE SKIP LOCKED
            )
            UPDATE jobs j SET
              state = CASE WHEN j.attempt >= j.max_attempts THEN 'DEAD' ELSE 'PENDING' END,
              run_at = CASE WHEN j.attempt >= j.max_attempts THEN j.run_at ELSE now() END,
              finished_at = CASE WHEN j.attempt >= j.max_attempts THEN now() END,
              last_error = 'lease expired', lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
            FROM expired WHERE j.id = expired.id
            RETURNING j.id, j.queue, j.state
            """;

    /** Moves jobs with expired leases back to PENDING, or to DEAD when attempts are exhausted. Safe on many instances. */
    public List<ReapedJob> reapExpired(int limit) {
        return jdbc.query(REAP_SQL, Map.of("limit", limit), (rs, i) -> new ReapedJob(
                rs.getObject("id", UUID.class), rs.getString("queue"), JobState.valueOf(rs.getString("state"))));
    }

    // ------------------------------------------------------------------ DLQ

    /**
     * Gives a DEAD job {@code extraAttempts} more attempts. {@code attempt} is deliberately NOT reset: if it were,
     * a zombie still holding an old attempt number could match a reused token and overwrite the new execution.
     */
    public boolean replay(UUID id, int extraAttempts) {
        if (extraAttempts < 1) {
            throw new IllegalArgumentException("extraAttempts must be >= 1");
        }
        return jdbc.update("""
                UPDATE jobs SET state = 'PENDING', run_at = now(), max_attempts = attempt + :extra,
                                finished_at = NULL, updated_at = now()
                WHERE id = :id AND state = 'DEAD'
                """, Map.of("id", id, "extra", extraAttempts)) == 1;
    }

    public List<Job> listDead(String tenantId, String queue, int limit, int offset) {
        MapSqlParameterSource p = new MapSqlParameterSource().addValue("limit", limit).addValue("offset", offset);
        String sql = "SELECT * FROM jobs WHERE state = 'DEAD'" + deadFilter(tenantId, queue, p)
                + " ORDER BY finished_at DESC, id LIMIT :limit OFFSET :offset";
        return jdbc.query(sql, p, JOB_MAPPER);
    }

    public long countDead(String tenantId, String queue) {
        MapSqlParameterSource p = new MapSqlParameterSource();
        Long n = jdbc.queryForObject("SELECT count(*) FROM jobs WHERE state = 'DEAD'" + deadFilter(tenantId, queue, p),
                p, Long.class);
        return n == null ? 0 : n;
    }

    private static String deadFilter(String tenantId, String queue, MapSqlParameterSource p) {
        StringBuilder sb = new StringBuilder();
        if (tenantId != null) {
            sb.append(" AND tenant_id = :tenant");
            p.addValue("tenant", tenantId);
        }
        if (queue != null) {
            sb.append(" AND queue = :queue");
            p.addValue("queue", queue);
        }
        return sb.toString();
    }

    // ------------------------------------------------------------------ gauges

    /** Per-queue counts for the gauges. Each branch hits a partial index (claim, lease, dead), never the whole table. */
    public Map<String, QueueStats> queueStats() {
        Map<String, long[]> acc = new LinkedHashMap<>();
        jdbc.query("""
                SELECT 0 AS kind, queue, count(*) AS n FROM jobs WHERE state = 'PENDING' AND run_at <= now() GROUP BY queue
                UNION ALL
                SELECT 1, queue, count(*) FROM jobs WHERE state = 'RUNNING' GROUP BY queue
                UNION ALL
                SELECT 2, queue, count(*) FROM jobs WHERE state = 'DEAD' GROUP BY queue
                """, new MapSqlParameterSource(), (RowMapper<Void>) (rs, i) -> {
                    long[] counts = acc.computeIfAbsent(rs.getString("queue"), k -> new long[3]);
                    counts[rs.getInt("kind")] = rs.getLong("n");
                    return null;
                });
        Map<String, QueueStats> out = new LinkedHashMap<>();
        acc.forEach((q, c) -> out.put(q, new QueueStats(q, c[0], c[1], c[2])));
        return out;
    }

    public record QueueStats(String queue, long depth, long running, long dead) {
    }

    // ------------------------------------------------------------------ mapping

    private static String truncate(String s) {
        if (s == null) {
            return null;
        }
        return s.length() <= MAX_ERROR_CHARS ? s : s.substring(0, MAX_ERROR_CHARS);
    }

    static final RowMapper<Job> JOB_MAPPER = JobRepository::mapJob;

    private static Job mapJob(ResultSet rs, int rowNum) throws SQLException {
        return new Job(
                rs.getObject("id", UUID.class),
                rs.getString("tenant_id"),
                rs.getString("queue"),
                rs.getString("type"),
                rs.getString("payload"),
                JobState.valueOf(rs.getString("state")),
                rs.getInt("priority"),
                instant(rs, "run_at"),
                rs.getInt("attempt"),
                rs.getInt("max_attempts"),
                rs.getString("lease_owner"),
                instant(rs, "lease_expires_at"),
                rs.getString("idempotency_key"),
                rs.getString("last_error"),
                instant(rs, "created_at"),
                instant(rs, "updated_at"),
                instant(rs, "started_at"),
                instant(rs, "finished_at"));
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime t = rs.getObject(column, OffsetDateTime.class);
        return t == null ? null : t.toInstant();
    }
}
