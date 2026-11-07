package io.relay.worker.bench;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.relay.core.EnqueueRequest;
import io.relay.core.JobRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Direct batch-insert load generator (bypasses the REST layer, measures PostgreSQL + JobRepository.enqueueBatch).
 * Env: DB_URL DB_USER DB_PASSWORD, BENCH_JOBS (default 100000), BENCH_BATCH (1000), BENCH_THREADS (4),
 * BENCH_QUEUE (bench), BENCH_TYPE (noop), BENCH_TENANT (bench).
 * Prints one line: {@code enqueued=<n> seconds=<s> jobs_per_sec=<r>}.
 */
public final class EnqueueBench {

    private EnqueueBench() {
    }

    public static void main(String[] args) throws Exception {
        long jobs = Long.parseLong(env("BENCH_JOBS", "100000"));
        int batch = Integer.parseInt(env("BENCH_BATCH", "1000"));
        int threads = Integer.parseInt(env("BENCH_THREADS", "4"));
        String queue = env("BENCH_QUEUE", "bench");
        String type = env("BENCH_TYPE", "noop");
        String tenant = env("BENCH_TENANT", "bench");
        String runId = UUID.randomUUID().toString().substring(0, 8);

        HikariConfig cfg = new HikariConfig();
        String url = env("DB_URL", "jdbc:postgresql://localhost:5432/relay");
        cfg.setJdbcUrl(url + (url.contains("?") ? "&" : "?") + "reWriteBatchedInserts=true");
        cfg.setUsername(env("DB_USER", "relay"));
        cfg.setPassword(env("DB_PASSWORD", "relay"));
        cfg.setMaximumPoolSize(threads);
        try (HikariDataSource ds = new HikariDataSource(cfg)) {
            JobRepository repo = new JobRepository(new NamedParameterJdbcTemplate(ds));
            AtomicLong next = new AtomicLong();
            AtomicLong inserted = new AtomicLong();
            long start = System.nanoTime();
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                futures.add(pool.submit(() -> {
                    while (true) {
                        long from = next.getAndAdd(batch);
                        if (from >= jobs) {
                            return;
                        }
                        long to = Math.min(jobs, from + batch);
                        List<EnqueueRequest> reqs = new ArrayList<>();
                        for (long i = from; i < to; i++) {
                            reqs.add(EnqueueRequest.of(tenant, queue, type, "{\"i\":" + i + ",\"pad\":\"xxxxxxxxxxxxxxxx\"}")
                                    .withIdempotencyKey("b-" + runId + "-" + i));
                        }
                        inserted.addAndGet(repo.enqueueBatch(reqs));
                    }
                }));
            }
            for (Future<?> f : futures) {
                f.get();
            }
            pool.shutdown();
            double secs = (System.nanoTime() - start) / 1e9;
            System.out.printf("enqueued=%d seconds=%.2f jobs_per_sec=%.0f%n", inserted.get(), secs, inserted.get() / secs);
        }
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }
}
