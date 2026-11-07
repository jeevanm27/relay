package io.relay.worker.app;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import com.sun.net.httpserver.HttpServer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.relay.core.JobRepository;
import io.relay.core.RetryPolicy;
import io.relay.worker.JobHandler;
import io.relay.worker.Worker;
import io.relay.worker.WorkerConfig;
import io.relay.worker.WorkerListener;
import io.relay.worker.bench.ChaosHandler;
import io.relay.worker.bench.ChaosListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Example worker application. Configured through environment variables so the same image runs in Docker Compose,
 * Kubernetes and the chaos harness. Workers talk to PostgreSQL directly through relay-core.
 */
public final class WorkerMain {

    private static final Logger log = LoggerFactory.getLogger(WorkerMain.class);

    private WorkerMain() {
    }

    public static void main(String[] args) throws Exception {
        String queue = env("RELAY_QUEUE", "default");
        String workerId = env("RELAY_WORKER_ID", InetAddress.getLocalHost().getHostName());
        int concurrency = Integer.parseInt(env("RELAY_CONCURRENCY", "10"));
        double leaseSecs = Double.parseDouble(env("RELAY_LEASE_SECS", "30"));
        int claimBatch = Integer.parseInt(env("RELAY_CLAIM_BATCH", "50"));
        long graceSecs = Long.parseLong(env("RELAY_SHUTDOWN_GRACE_SECS", "30"));
        long idleMinMs = Long.parseLong(env("RELAY_IDLE_MIN_MS", "50"));
        long idleMaxMs = Long.parseLong(env("RELAY_IDLE_MAX_MS", "250"));
        int metricsPort = Integer.parseInt(env("METRICS_PORT", "9404"));
        boolean chaos = Boolean.parseBoolean(env("RELAY_CHAOS", "false"));
        RetryPolicy retry = new RetryPolicy(Double.parseDouble(env("RELAY_RETRY_BASE_SECS", "1")),
                Double.parseDouble(env("RELAY_RETRY_CAP_SECS", "300")));

        HikariConfig pool = new HikariConfig();
        pool.setJdbcUrl(env("DB_URL", "jdbc:postgresql://localhost:5432/relay"));
        pool.setUsername(env("DB_USER", "relay"));
        pool.setPassword(env("DB_PASSWORD", "relay"));
        // claim + heartbeat + complete/fail need a few connections; chaos handlers also use one each while running
        pool.setMaximumPoolSize(Integer.parseInt(env("RELAY_POOL_SIZE", String.valueOf(concurrency + 4))));
        pool.setInitializationFailTimeout(-1);
        pool.setPoolName("relay-" + workerId);
        HikariDataSource ds = new HikariDataSource(pool);
        NamedParameterJdbcTemplate jdbc = new NamedParameterJdbcTemplate(ds);
        waitForSchema(jdbc);

        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        JobRepository repo = new JobRepository(jdbc, retry);

        Map<String, JobHandler> handlers = new HashMap<>();
        handlers.put("log", new LogPayloadHandler());
        handlers.put("noop", ctx -> { });
        handlers.put("sleep50", ctx -> Thread.sleep(50));
        handlers.put("sleep10s", ctx -> Thread.sleep(10_000)); // long job, used by the recovery-time benchmark
        WorkerListener listener = WorkerListener.NONE;
        if (chaos) {
            long slowMs = Long.parseLong(env("CHAOS_SLOW_MS", "15000"));
            handlers.put("chaos", new ChaosHandler(ds, workerId, 0));
            handlers.put("chaos-slow", new ChaosHandler(ds, workerId, slowMs));
            listener = new ChaosListener(ds, workerId);
            log.warn("CHAOS MODE: chaos handlers and the acceptance audit listener are enabled");
        }

        WorkerConfig config = WorkerConfig.forQueue(queue)
                .withWorkerId(workerId)
                .withMaxConcurrency(concurrency)
                .withLeaseSecs(leaseSecs)
                .withClaimBatch(claimBatch)
                .withIdle(Duration.ofMillis(idleMinMs), Duration.ofMillis(idleMaxMs))
                .withShutdownGrace(Duration.ofSeconds(graceSecs));

        Worker worker = new Worker(repo, config, handlers, registry, listener);
        HttpServer metricsServer = startMetricsServer(metricsPort, registry);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("SIGTERM/shutdown received");
            worker.stop();
            metricsServer.stop(0);
            ds.close();
        }, "relay-shutdown"));

        worker.start();
        Thread.currentThread().join();
    }

    private static void waitForSchema(NamedParameterJdbcTemplate jdbc) throws InterruptedException {
        for (int i = 0; i < 120; i++) {
            try {
                jdbc.getJdbcTemplate().queryForObject("SELECT count(*) FROM (SELECT 1 FROM jobs LIMIT 1) s", Long.class);
                return;
            } catch (RuntimeException e) {
                if (i % 5 == 0) {
                    log.info("waiting for database schema (relay-server runs the migrations): {}", e.getMessage());
                }
                Thread.sleep(1000);
            }
        }
        throw new IllegalStateException("database schema not available after 120s");
    }

    private static HttpServer startMetricsServer(int port, PrometheusMeterRegistry registry) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/metrics", exchange -> {
            byte[] body = registry.scrape().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.createContext("/healthz", exchange -> {
            byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.start();
        return server;
    }

    private static String env(String key, String defaultValue) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? defaultValue : v;
    }
}
