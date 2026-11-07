package io.relay.worker;

import java.time.Duration;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/**
 * Worker-side metrics. Labels are limited to queue and outcome: never tenant or job id (cardinality).
 * Prometheus names: relay_jobs_finished_total, relay_job_pickup_seconds, relay_job_duration_seconds,
 * relay_fencing_rejections_total.
 */
public final class WorkerMetrics {

    private final MeterRegistry registry;
    private final String queue;
    private final Timer pickup;
    private final Timer duration;
    private final Counter fencingRejections;
    private final Counter leaseLost;

    public WorkerMetrics(MeterRegistry registry, String queue) {
        this.registry = registry;
        this.queue = queue;
        this.pickup = Timer.builder("relay.job.pickup")
                .description("Time from enqueue to first claim (database clock)")
                .tag("queue", queue)
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofMinutes(10))
                .register(registry);
        this.duration = Timer.builder("relay.job.duration")
                .description("Handler execution time")
                .tag("queue", queue)
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofMinutes(10))
                .register(registry);
        this.fencingRejections = Counter.builder("relay.fencing.rejections")
                .description("complete()/fail() calls rejected because the attempt token was stale")
                .register(registry);
        this.leaseLost = Counter.builder("relay.worker.leases.lost")
                .description("Heartbeats that found the lease already lost")
                .register(registry);
    }

    public void pickup(Duration d) {
        pickup.record(d.isNegative() ? Duration.ZERO : d);
    }

    public void duration(Duration d) {
        duration.record(d);
    }

    public void finished(String outcome) {
        Counter.builder("relay.jobs.finished")
                .description("Jobs finished by this worker, by outcome (succeeded|retried|dead)")
                .tag("queue", queue)
                .tag("outcome", outcome)
                .register(registry)
                .increment();
    }

    public void fencingRejected() {
        fencingRejections.increment();
    }

    public void leaseLost() {
        leaseLost.increment();
    }
}
