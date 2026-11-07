package io.relay.worker.bench;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;

import javax.sql.DataSource;

import io.relay.worker.JobContext;
import io.relay.worker.JobHandler;

/**
 * Chaos-test handler (bench only; enabled with RELAY_CHAOS=true). It is what an idempotent handler looks like:
 * <ol>
 *   <li>append (job_id, attempt, worker_id) to execution_log (every execution leaves a trace, so duplicates are countable)</li>
 *   <li>sleep 5 to 50 ms, or {@code slowMs} for the deliberately slow "chaos-slow" jobs</li>
 *   <li>INSERT INTO side_effects(job_id) ... ON CONFLICT DO NOTHING (the idempotent side effect)</li>
 * </ol>
 * Each JDBC call borrows a connection only for that statement; no transaction spans the sleep.
 */
public final class ChaosHandler implements JobHandler {

    private final DataSource ds;
    private final String workerId;
    private final long slowMs;

    public ChaosHandler(DataSource ds, String workerId, long slowMs) {
        this.ds = ds;
        this.workerId = workerId;
        this.slowMs = slowMs;
    }

    @Override
    public void handle(JobContext ctx) throws Exception {
        execute("INSERT INTO execution_log (job_id, attempt, worker_id) VALUES (?, ?, ?)",
                ctx.jobId(), ctx.attempt(), workerId);
        Thread.sleep(slowMs > 0 ? slowMs : ThreadLocalRandom.current().nextLong(5, 51));
        execute("INSERT INTO side_effects (job_id) VALUES (?) ON CONFLICT DO NOTHING", ctx.jobId());
    }

    private void execute(String sql, Object... args) throws SQLException {
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i + 1, args[i]);
            }
            ps.executeUpdate();
        }
    }
}
