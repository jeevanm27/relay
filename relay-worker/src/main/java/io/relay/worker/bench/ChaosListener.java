package io.relay.worker.bench;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import io.relay.core.FailResult;
import io.relay.core.Job;
import io.relay.worker.WorkerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Audit trail for the chaos invariants. It writes to the database (not to in-memory counters) because a kill -9
 * destroys everything the JVM held.
 *
 * <ul>
 *   <li>{@code acceptance_log}: whenever the database ACCEPTS a completion, the attempt the worker believed it was
 *       completing. If fencing were broken, a zombie holding attempt 1 would be accepted while the job is already on
 *       attempt 2, and the verifier (I5b) would see accepted attempt 1 != final jobs.attempt 2.</li>
 *   <li>{@code chaos_events}: fencing rejections and lease-lost interrupts, for the final summary.</li>
 * </ul>
 *
 * <p>A kill -9 between the commit of complete() and the audit insert loses one audit row. That only weakens the
 * check for that job; it can never produce a false failure.
 */
public final class ChaosListener implements WorkerListener {

    private static final Logger log = LoggerFactory.getLogger(ChaosListener.class);

    private final DataSource ds;
    private final String workerId;

    public ChaosListener(DataSource ds, String workerId) {
        this.ds = ds;
        this.workerId = workerId;
    }

    @Override
    public void onComplete(Job job, boolean accepted) {
        if (accepted) {
            write("INSERT INTO acceptance_log (job_id, attempt, worker_id) VALUES (?, ?, ?)", job, null);
        } else {
            write("INSERT INTO chaos_events (kind, job_id, attempt, worker_id) VALUES (?, ?, ?, ?)", job,
                    "fencing_rejection");
        }
    }

    @Override
    public void onFail(Job job, FailResult result) {
        if (!result.accepted()) {
            write("INSERT INTO chaos_events (kind, job_id, attempt, worker_id) VALUES (?, ?, ?, ?)", job,
                    "fencing_rejection");
        }
    }

    @Override
    public void onLeaseLost(Job job) {
        write("INSERT INTO chaos_events (kind, job_id, attempt, worker_id) VALUES (?, ?, ?, ?)", job, "lease_lost");
    }

    private void write(String sql, Job job, String kind) {
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            int i = 1;
            if (kind != null) {
                ps.setString(i++, kind);
            }
            ps.setObject(i++, job.id());
            ps.setInt(i++, job.attempt());
            ps.setString(i, workerId);
            ps.executeUpdate();
        } catch (SQLException e) {
            log.warn("could not write chaos audit row for {}: {}", job.id(), e.getMessage());
        }
    }
}
