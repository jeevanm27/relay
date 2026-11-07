package io.relay.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import io.relay.testing.PostgresSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/** Phase 2 test (c): poison pill reaches DEAD via repeated lease expiry; reaper is safe on several instances. */
class PoisonPillAndReaperTest {

    private JobRepository repo;
    private NamedParameterJdbcTemplate jdbc;

    @BeforeEach
    void setUp() {
        PostgresSupport.reset();
        jdbc = PostgresSupport.jdbc();
        repo = new JobRepository(jdbc);
    }

    @Test
    void jobThatKillsEveryWorkerEndsDeadThroughLeaseExpiry() throws Exception {
        int maxAttempts = 3;
        UUID id = repo.enqueue(EnqueueRequest.of("acme", "q", "poison", null).withMaxAttempts(maxAttempts)).job().id();

        for (int cycle = 1; cycle <= maxAttempts; cycle++) {
            // a worker claims it and "dies": it never heartbeats and never completes or fails
            Job job = repo.claim("q", "doomed-worker-" + cycle, 0.3, 1).get(0);
            assertThat(job.attempt()).isEqualTo(cycle);
            Thread.sleep(600);
            List<ReapedJob> reaped = repo.reapExpired(500);
            assertThat(reaped).hasSize(1);
            assertThat(reaped.get(0).newState())
                    .isEqualTo(cycle < maxAttempts ? JobState.PENDING : JobState.DEAD);
        }

        Job dead = repo.findById(id).orElseThrow();
        assertThat(dead.state()).isEqualTo(JobState.DEAD);
        assertThat(dead.attempt()).isEqualTo(maxAttempts);
        assertThat(dead.lastError()).isEqualTo("lease expired");
        assertThat(dead.finishedAt()).isNotNull();
        assertThat(dead.leaseOwner()).isNull();
        assertThat(repo.claim("q", "w", 30, 1)).isEmpty();
    }

    @Test
    void severalReaperInstancesNeverReapTheSameJobTwice() throws Exception {
        int jobs = 600;
        jdbc.getJdbcTemplate().execute("INSERT INTO tenants (id) VALUES ('t')");
        jdbc.getJdbcTemplate().update("""
                INSERT INTO jobs (tenant_id, queue, type, state, attempt, lease_owner, lease_expires_at)
                SELECT 't', 'q', 'x', 'RUNNING', 1, 'gone', now() - interval '10 seconds' FROM generate_series(1, ?)
                """, jobs);

        int reapers = 8;
        CountDownLatch startGun = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(reapers);
        List<Future<List<ReapedJob>>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < reapers; i++) {
                futures.add(pool.submit(() -> {
                    startGun.await();
                    List<ReapedJob> mine = new ArrayList<>();
                    List<ReapedJob> batch;
                    do {
                        batch = repo.reapExpired(50);
                        mine.addAll(batch);
                    } while (!batch.isEmpty());
                    return mine;
                }));
            }
            startGun.countDown();
            Set<UUID> seen = new HashSet<>();
            int total = 0;
            for (Future<List<ReapedJob>> f : futures) {
                for (ReapedJob r : f.get(60, TimeUnit.SECONDS)) {
                    total++;
                    assertThat(seen.add(r.id())).as("job %s reaped twice", r.id()).isTrue();
                }
            }
            assertThat(total).isEqualTo(jobs);
        } finally {
            pool.shutdownNow();
        }
        assertThat(jdbc.getJdbcTemplate().queryForObject(
                "SELECT count(*) FROM jobs WHERE state = 'PENDING' AND attempt = 1", Long.class)).isEqualTo(jobs);
    }
}
