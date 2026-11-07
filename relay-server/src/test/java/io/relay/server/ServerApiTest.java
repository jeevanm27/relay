package io.relay.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import io.relay.core.Job;
import io.relay.core.JobRepository;
import io.relay.server.ops.QueueGauges;
import io.relay.server.ops.ReaperTask;
import io.relay.testing.PostgresSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

@AutoConfigureObservability // Spring Boot disables metrics export in tests unless asked
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        // scheduled tasks are triggered explicitly in these tests; keep the background ones out of the way
        "relay.reaper-interval-ms=3600000",
        "relay.gauge-interval-ms=3600000"
})
class ServerApiTest {

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", PostgresSupport::jdbcUrl);
        registry.add("spring.datasource.username", PostgresSupport::username);
        registry.add("spring.datasource.password", PostgresSupport::password);
    }

    @Autowired
    TestRestTemplate http;
    @Autowired
    JobRepository repo;
    @Autowired
    ReaperTask reaper;
    @Autowired
    QueueGauges gauges;

    @BeforeEach
    void clean() {
        PostgresSupport.reset();
    }

    private Map<String, Object> body(String tenant, String queue, String key) {
        var m = new java.util.HashMap<String, Object>();
        m.put("tenant", tenant);
        m.put("queue", queue);
        m.put("type", "send-email");
        m.put("payload", Map.of("to", "a@b.c", "n", 1));
        if (key != null) {
            m.put("idempotencyKey", key);
        }
        return m;
    }

    @Test
    void postCreatesJobAndAutoCreatesTenant() {
        ResponseEntity<JsonNode> res = http.postForEntity("/v1/jobs", body("new-tenant", "mail", null), JsonNode.class);

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(res.getHeaders().getLocation()).isNotNull();
        JsonNode job = res.getBody();
        assertThat(job.get("state").asText()).isEqualTo("PENDING");
        assertThat(job.get("attempt").asInt()).isZero();
        assertThat(job.get("maxAttempts").asInt()).isEqualTo(5);
        assertThat(job.get("payload").get("to").asText()).isEqualTo("a@b.c");
        assertThat(PostgresSupport.jdbc().getJdbcTemplate()
                .queryForObject("SELECT count(*) FROM tenants WHERE id = 'new-tenant'", Long.class)).isEqualTo(1L);

        ResponseEntity<JsonNode> got = http.getForEntity("/v1/jobs/" + job.get("id").asText(), JsonNode.class);
        assertThat(got.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(got.getBody().get("id").asText()).isEqualTo(job.get("id").asText());
    }

    @Test
    void duplicateIdempotencyKeyReturns200WithTheExistingJob() {
        ResponseEntity<JsonNode> first = http.postForEntity("/v1/jobs", body("acme", "mail", "order-1"), JsonNode.class);
        ResponseEntity<JsonNode> second = http.postForEntity("/v1/jobs", body("acme", "mail", "order-1"), JsonNode.class);

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getBody().get("id").asText()).isEqualTo(first.getBody().get("id").asText());
        assertThat(PostgresSupport.jdbc().getJdbcTemplate().queryForObject("SELECT count(*) FROM jobs", Long.class))
                .isEqualTo(1L);
    }

    @Test
    void concurrentDuplicatesOverHttpCreateOneRow() throws Exception {
        int threads = 20;
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<HttpStatus>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    go.await();
                    return HttpStatus.valueOf(http.postForEntity("/v1/jobs", body("acme", "mail", "same"), JsonNode.class)
                            .getStatusCode().value());
                }));
            }
            go.countDown();
            long created = 0;
            long ok = 0;
            for (Future<HttpStatus> f : futures) {
                HttpStatus s = f.get(60, TimeUnit.SECONDS);
                if (s == HttpStatus.CREATED) {
                    created++;
                } else if (s == HttpStatus.OK) {
                    ok++;
                }
            }
            assertThat(created).isEqualTo(1);
            assertThat(ok).isEqualTo(threads - 1);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void invalidInputIsRejectedWith400() {
        assertThat(http.postForEntity("/v1/jobs", Map.of("queue", "q", "type", "t"), String.class).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(http.postForEntity("/v1/jobs", Map.of("tenant", "has space", "queue", "q", "type", "t"), String.class)
                .getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        var tooMany = body("acme", "q", null);
        tooMany.put("maxAttempts", 100000);
        assertThat(http.postForEntity("/v1/jobs", tooMany, String.class).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(http.postForEntity("/v1/jobs", new HttpEntity<>("{not json"), String.class).getStatusCode().value())
                .isIn(400, 415);
    }

    @Test
    void unknownJobIs404() {
        assertThat(http.getForEntity("/v1/jobs/" + UUID.randomUUID(), String.class).getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void dlqListReplayAndConflicts() {
        UUID id = UUID.fromString(http.postForEntity("/v1/jobs", body("acme", "mail", null), JsonNode.class)
                .getBody().get("id").asText());

        // replaying a job that is not DEAD is a conflict
        assertThat(http.postForEntity("/v1/dlq/" + id + "/replay", Map.of("extraAttempts", 2), String.class)
                .getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(http.postForEntity("/v1/dlq/" + UUID.randomUUID() + "/replay", Map.of("extraAttempts", 2), String.class)
                .getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);

        // kill it: claim, then non-retryable fail
        Job claimed = repo.claim("mail", "w", 30, 1).get(0);
        repo.fail(id, claimed.attempt(), "boom", false);

        ResponseEntity<JsonNode> list = http.getForEntity("/v1/dlq?tenant=acme&queue=mail", JsonNode.class);
        assertThat(list.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(list.getBody().get("total").asLong()).isEqualTo(1);
        assertThat(list.getBody().get("items").get(0).get("id").asText()).isEqualTo(id.toString());
        assertThat(list.getBody().get("items").get(0).get("lastError").asText()).isEqualTo("boom");
        assertThat(http.getForEntity("/v1/dlq?tenant=other", JsonNode.class).getBody().get("total").asLong()).isZero();
        assertThat(http.getForEntity("/v1/dlq?size=0", String.class).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

        assertThat(http.postForEntity("/v1/dlq/" + id + "/replay", Map.of("extraAttempts", 0), String.class)
                .getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        ResponseEntity<JsonNode> replay = http.postForEntity("/v1/dlq/" + id + "/replay",
                Map.of("extraAttempts", 3), JsonNode.class);
        assertThat(replay.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(replay.getBody().get("state").asText()).isEqualTo("PENDING");
        assertThat(replay.getBody().get("attempt").asInt()).isEqualTo(1);
        assertThat(replay.getBody().get("maxAttempts").asInt()).isEqualTo(4);
    }

    @Test
    void prometheusEndpointExposesTheRelayMetrics() {
        http.postForEntity("/v1/jobs", body("acme", "metrics-q", null), JsonNode.class);
        http.postForEntity("/v1/jobs", body("acme", "metrics-q", null), JsonNode.class);
        Job doomed = repo.claim("metrics-q", "w", 0.2, 1).get(0);
        assertThat(doomed).isNotNull();
        try {
            Thread.sleep(400);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertThat(reaper.reapOnce()).isEqualTo(1);
        gauges.refresh();

        String text = http.getForObject("/actuator/prometheus", String.class);
        assertThat(text)
                .contains("relay_jobs_enqueued_total{")
                .contains("queue=\"metrics-q\"")
                .contains("relay_lease_expirations_total")
                .contains("relay_queue_depth{")
                .contains("relay_running_jobs{")
                .contains("relay_dlq_size{");
        assertThat(line(text, "relay_queue_depth", "metrics-q")).endsWith(" 2.0");
        assertThat(line(text, "relay_running_jobs", "metrics-q")).endsWith(" 0.0");
        assertThat(line(text, "relay_jobs_enqueued_total", "metrics-q")).endsWith(" 2.0");
        // no tenant or job id label anywhere in relay metrics
        assertThat(text.lines().filter(l -> l.startsWith("relay_")).toList())
                .noneMatch(l -> l.contains("tenant=") || l.contains("job_id=") || l.contains("id="));
    }

    private static String line(String text, String metric, String queue) {
        return text.lines()
                .filter(l -> l.startsWith(metric + "{") && l.contains("queue=\"" + queue + "\""))
                .findFirst().orElseThrow(() -> new AssertionError("no line for " + metric + " " + queue));
    }
}
