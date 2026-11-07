package io.relay.server.ops;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.relay.core.JobRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Refreshes per-queue gauges from the database every few seconds, instead of querying on every Prometheus scrape.
 * Label cardinality is bounded by the number of queues; there is deliberately no tenant or job id label.
 */
@Component
public class QueueGauges {

    private static final Logger log = LoggerFactory.getLogger(QueueGauges.class);

    private final JobRepository repo;
    private final MeterRegistry registry;
    private final Map<String, AtomicLong> depth = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> running = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> dlq = new ConcurrentHashMap<>();

    public QueueGauges(JobRepository repo, MeterRegistry registry) {
        this.repo = repo;
        this.registry = registry;
    }

    @Scheduled(fixedDelayString = "${relay.gauge-interval-ms:5000}", initialDelay = 1000)
    public void run() {
        try {
            refresh();
        } catch (RuntimeException e) {
            log.warn("gauge refresh failed: {}", e.toString());
        }
    }

    public void refresh() {
        Map<String, JobRepository.QueueStats> stats = repo.queueStats();
        Set<String> queues = new HashSet<>(stats.keySet());
        queues.addAll(depth.keySet()); // queues that emptied out must be reset to 0, not left stale
        for (String q : queues) {
            JobRepository.QueueStats s = stats.get(q);
            set(depth, "relay.queue.depth", "Jobs PENDING and due (run_at <= now())", q, s == null ? 0 : s.depth());
            set(running, "relay.running.jobs", "Jobs currently RUNNING", q, s == null ? 0 : s.running());
            set(dlq, "relay.dlq.size", "Jobs in DEAD state", q, s == null ? 0 : s.dead());
        }
    }

    private void set(Map<String, AtomicLong> holders, String name, String description, String queue, long value) {
        holders.computeIfAbsent(queue, q -> {
            AtomicLong holder = new AtomicLong();
            Gauge.builder(name, holder, AtomicLong::get).description(description).tag("queue", q).register(registry);
            return holder;
        }).set(value);
    }
}
