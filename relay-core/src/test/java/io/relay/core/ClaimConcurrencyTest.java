package io.relay.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.relay.testing.PostgresSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/** Phase 1 test (a): 1,000 jobs, 100 concurrent claimers, every job is claimed exactly once. */
class ClaimConcurrencyTest {

    private static final int JOBS = 1_000;
    private static final int CLAIMERS = 100;

    private JobRepository repo;
    private NamedParameterJdbcTemplate jdbc;

    @BeforeEach
    void setUp() {
        PostgresSupport.reset();
        jdbc = PostgresSupport.jdbc();
        repo = new JobRepository(jdbc);
    }

    /** Run with -Drelay.claim.rounds=20 to repeat the whole scenario (flaky concurrency tests are the bug to hunt). */
    @Test
    void everyJobIsClaimedExactlyOnceByHundredConcurrentClaimers() throws Exception {
        int rounds = Integer.getInteger("relay.claim.rounds", 1);
        for (int round = 1; round <= rounds; round++) {
            PostgresSupport.reset();
            runOneRound();
        }
    }

    private void runOneRound() throws Exception {
        seed(JOBS);

        ConcurrentHashMap<UUID, AtomicInteger> claimCounts = new ConcurrentHashMap<>();
        CountDownLatch startGun = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(CLAIMERS);
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int c = 0; c < CLAIMERS; c++) {
                String workerId = "claimer-" + c;
                // mix batch sizes so both single-row and multi-row claims race against each other
                int batch = (c % 3 == 0) ? 1 : (c % 3 == 1) ? 7 : 25;
                futures.add(pool.submit(() -> {
                    startGun.await();
                    while (true) {
                        List<Job> got = repo.claim("q", workerId, 60, batch);
                        if (got.isEmpty()) {
                            return null;
                        }
                        for (Job j : got) {
                            claimCounts.computeIfAbsent(j.id(), k -> new AtomicInteger()).incrementAndGet();
                            assertThat(j.attempt()).isEqualTo(1);
                            assertThat(j.leaseOwner()).isEqualTo(workerId);
                        }
                    }
                }));
            }
            startGun.countDown();
            for (Future<?> f : futures) {
                f.get(120, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        assertThat(claimCounts).hasSize(JOBS);
        assertThat(claimCounts.values()).allSatisfy(n -> assertThat(n.get()).isEqualTo(1));

        // the database agrees: all RUNNING, attempt = 1 everywhere, nothing left PENDING
        assertThat(count("state = 'RUNNING' AND attempt = 1")).isEqualTo(JOBS);
        assertThat(count("state = 'PENDING'")).isZero();
    }

    private void seed(int n) {
        jdbc.getJdbcTemplate().execute("INSERT INTO tenants (id) VALUES ('t1')");
        jdbc.getJdbcTemplate().update("""
                INSERT INTO jobs (tenant_id, queue, type, payload)
                SELECT 't1', 'q', 'noop', '{}'::jsonb FROM generate_series(1, ?)
                """, n);
    }

    private long count(String where) {
        Long n = jdbc.getJdbcTemplate().queryForObject("SELECT count(*) FROM jobs WHERE " + where, Long.class);
        return n == null ? 0 : n;
    }
}
