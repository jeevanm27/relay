package io.relay.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import io.relay.testing.PostgresSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/** Phase 2 test (a): the zombie test. The fencing condition is {@code WHERE id = :id AND attempt = :attempt AND state = 'RUNNING'}. */
class ZombieFencingTest {

    private JobRepository repo;
    private NamedParameterJdbcTemplate jdbc;

    @BeforeEach
    void setUp() {
        PostgresSupport.reset();
        jdbc = PostgresSupport.jdbc();
        repo = new JobRepository(jdbc);
    }

    @Test
    void zombieCannotOverwriteTheWorkerThatTookOver() throws Exception {
        UUID id = repo.enqueue(EnqueueRequest.of("acme", "q", "work", null)).job().id();
        AtomicInteger acceptedCompletions = new AtomicInteger();

        // worker A claims with a 1 second lease, then stalls (GC pause, SIGSTOP, network partition ...)
        Job a = repo.claim("q", "worker-A", 1, 1).get(0);
        assertThat(a.attempt()).isEqualTo(1);

        Thread.sleep(2_000);

        // the reaper notices the expired lease; the job becomes claimable again
        List<ReapedJob> reaped = repo.reapExpired(500);
        assertThat(reaped).extracting(ReapedJob::id).containsExactly(id);
        assertThat(reaped.get(0).newState()).isEqualTo(JobState.PENDING);

        // worker B claims the job: attempt = 2
        Job b = repo.claim("q", "worker-B", 30, 1).get(0);
        assertThat(b.id()).isEqualTo(id);
        assertThat(b.attempt()).isEqualTo(2);

        // zombie A wakes up while B holds the job. Every fenced call must be rejected.
        assertThat(repo.heartbeat(id, a.attempt(), 30)).isFalse();
        assertThat(repo.fail(id, a.attempt(), "zombie failure", true)).isEqualTo(FailResult.STALE);
        if (repo.complete(id, a.attempt())) {
            acceptedCompletions.incrementAndGet();
        }
        assertThat(acceptedCompletions.get()).isZero();

        Job stillB = repo.findById(id).orElseThrow();
        assertThat(stillB.state()).isEqualTo(JobState.RUNNING);
        assertThat(stillB.leaseOwner()).isEqualTo("worker-B");
        assertThat(stillB.lastError()).doesNotContain("zombie");

        // B finishes: accepted
        assertThat(repo.heartbeat(id, b.attempt(), 30)).isTrue();
        if (repo.complete(id, b.attempt())) {
            acceptedCompletions.incrementAndGet();
        }
        assertThat(acceptedCompletions.get()).isEqualTo(1);

        Job done = repo.findById(id).orElseThrow();
        assertThat(done.state()).isEqualTo(JobState.SUCCEEDED);
        assertThat(done.attempt()).isEqualTo(2);
        assertThat(done.finishedAt()).isNotNull();

        // a second late call from either worker changes nothing
        assertThat(repo.complete(id, b.attempt())).isFalse();
        assertThat(repo.complete(id, a.attempt())).isFalse();
    }

    @Test
    void completeOnNonRunningJobIsRejectedEvenWithMatchingAttempt() {
        UUID id = repo.enqueue(EnqueueRequest.of("acme", "q", "work", null)).job().id();
        // never claimed: attempt is 0, state PENDING
        assertThat(repo.complete(id, 0)).isFalse();
        assertThat(repo.heartbeat(id, 0, 30)).isFalse();
        assertThat(repo.fail(id, 0, "x", true)).isEqualTo(FailResult.STALE);
    }

    @Test
    void reaperLeavesLiveLeasesAlone() {
        UUID id = repo.enqueue(EnqueueRequest.of("acme", "q", "work", null)).job().id();
        repo.claim("q", "w", 60, 1);
        assertThat(repo.reapExpired(500)).isEmpty();
        assertThat(repo.findById(id).orElseThrow().state()).isEqualTo(JobState.RUNNING);
    }

    @Test
    void heartbeatKeepsALeaseAliveAndExtendsIt() throws Exception {
        UUID id = repo.enqueue(EnqueueRequest.of("acme", "q", "work", null)).job().id();
        Job job = repo.claim("q", "w", 1, 1).get(0);
        for (int i = 0; i < 4; i++) {
            Thread.sleep(500);
            assertThat(repo.heartbeat(id, job.attempt(), 1)).isTrue();
        }
        // 2 seconds have passed, longer than the 1 second lease, but heartbeats kept extending it
        assertThat(repo.reapExpired(500)).isEmpty();
        assertThat(repo.complete(id, job.attempt())).isTrue();
    }
}
