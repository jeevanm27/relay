package io.relay.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import io.relay.testing.PostgresSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/** Phase 1 test (b) plus enqueue edge cases. */
class IdempotentEnqueueTest {

    private JobRepository repo;
    private NamedParameterJdbcTemplate jdbc;

    @BeforeEach
    void setUp() {
        PostgresSupport.reset();
        jdbc = PostgresSupport.jdbc();
        repo = new JobRepository(jdbc);
    }

    @Test
    void sameIdempotencyKeyEnqueuedFiftyTimesConcurrentlyCreatesExactlyOneRow() throws Exception {
        int threads = 50;
        EnqueueRequest req = EnqueueRequest.of("acme", "emails", "send", "{\"to\":\"a@b.c\"}")
                .withIdempotencyKey("order-42");
        CountDownLatch startGun = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<EnqueueResult>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    startGun.await();
                    return repo.enqueue(req);
                }));
            }
            startGun.countDown();
            List<EnqueueResult> results = new ArrayList<>();
            for (Future<EnqueueResult> f : futures) {
                results.add(f.get(60, TimeUnit.SECONDS));
            }

            assertThat(results.stream().filter(EnqueueResult::created)).hasSize(1);
            assertThat(results.stream().map(r -> r.job().id()).distinct()).hasSize(1);
        } finally {
            pool.shutdownNow();
        }
        assertThat(jdbc.getJdbcTemplate().queryForObject("SELECT count(*) FROM jobs", Long.class)).isEqualTo(1L);
        assertThat(jdbc.getJdbcTemplate().queryForObject("SELECT count(*) FROM tenants", Long.class)).isEqualTo(1L);
    }

    @Test
    void sameKeyInDifferentTenantsAreDifferentJobs() {
        EnqueueRequest a = EnqueueRequest.of("tenant-a", "q", "t", null).withIdempotencyKey("k");
        EnqueueRequest b = EnqueueRequest.of("tenant-b", "q", "t", null).withIdempotencyKey("k");
        EnqueueResult ra = repo.enqueue(a);
        EnqueueResult rb = repo.enqueue(b);
        assertThat(ra.created()).isTrue();
        assertThat(rb.created()).isTrue();
        assertThat(ra.job().id()).isNotEqualTo(rb.job().id());
    }

    @Test
    void jobsWithoutKeyAreNeverDeduplicated() {
        EnqueueRequest r = EnqueueRequest.of("acme", "q", "t", "{}");
        assertThat(repo.enqueue(r).job().id()).isNotEqualTo(repo.enqueue(r).job().id());
    }

    @Test
    void duplicateReturnsTheOriginalJobNotTheNewParameters() {
        EnqueueResult first = repo.enqueue(EnqueueRequest.of("acme", "q", "t", "{\"v\":1}")
                .withIdempotencyKey("k").withPriority(3));
        EnqueueResult second = repo.enqueue(EnqueueRequest.of("acme", "q", "t", "{\"v\":2}")
                .withIdempotencyKey("k").withPriority(9));
        assertThat(second.created()).isFalse();
        assertThat(second.job().id()).isEqualTo(first.job().id());
        assertThat(second.job().priority()).isEqualTo(3);
        assertThat(second.job().payload()).contains("\"v\": 1");
    }

    @Test
    void newJobHasDefaults() {
        Job job = repo.enqueue(EnqueueRequest.of("acme", "q", "t", null)).job();
        assertThat(job.state()).isEqualTo(JobState.PENDING);
        assertThat(job.attempt()).isZero();
        assertThat(job.maxAttempts()).isEqualTo(5);
        assertThat(job.payload()).isEqualTo("{}");
        assertThat(job.createdAt()).isNotNull();
    }

    @Test
    void invalidInputIsRejectedBeforeTouchingTheDatabase() {
        assertThatThrownBy(() -> EnqueueRequest.of("", "q", "t", null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EnqueueRequest.of("a b", "q", "t", null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EnqueueRequest.of("a", "q", "t", null).withMaxAttempts(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EnqueueRequest.of("a", "q", "t", null).withIdempotencyKey(" "))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
