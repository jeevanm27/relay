package io.relay.server.ops;

import java.util.List;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.relay.core.JobRepository;
import io.relay.core.JobState;
import io.relay.core.ReapedJob;
import io.relay.server.RelayProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Requeues jobs whose lease expired. Runs on every server instance: the reaper statement uses
 * {@code FOR UPDATE SKIP LOCKED}, so concurrent instances take disjoint sets of rows and a job is reaped at most once
 * per expiry. Time comes from PostgreSQL ({@code lease_expires_at < now()}).
 */
@Component
public class ReaperTask {

    private static final Logger log = LoggerFactory.getLogger(ReaperTask.class);

    private final JobRepository repo;
    private final RelayProperties props;
    private final MeterRegistry registry;
    private final Counter leaseExpirations;

    public ReaperTask(JobRepository repo, RelayProperties props, MeterRegistry registry) {
        this.repo = repo;
        this.props = props;
        this.registry = registry;
        this.leaseExpirations = Counter.builder("relay.lease.expirations")
                .description("Jobs whose lease expired and were reaped")
                .register(registry);
    }

    @Scheduled(fixedDelayString = "${relay.reaper-interval-ms:5000}", initialDelayString = "${relay.reaper-interval-ms:5000}")
    public void run() {
        try {
            reapOnce();
        } catch (RuntimeException e) {
            // a database blip must not cancel the scheduled task
            log.warn("reaper run failed: {}", e.toString());
        }
    }

    /** Reaps until a batch comes back short (or a safety bound is hit). Returns the number of jobs reaped. */
    public int reapOnce() {
        int total = 0;
        for (int i = 0; i < props.maxReaperLoops(); i++) {
            List<ReapedJob> batch = repo.reapExpired(props.reaperBatch());
            for (ReapedJob r : batch) {
                leaseExpirations.increment();
                if (r.newState() == JobState.DEAD) {
                    Counter.builder("relay.jobs.finished")
                            .tag("queue", r.queue())
                            .tag("outcome", "dead")
                            .register(registry)
                            .increment();
                }
            }
            total += batch.size();
            if (batch.size() < props.reaperBatch()) {
                break;
            }
        }
        if (total > 0) {
            log.info("reaper: {} jobs with expired leases requeued or moved to DEAD", total);
        }
        return total;
    }
}
