package io.relay.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;

import io.relay.testing.PostgresSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/** Phase 1 test (c): priority and run_at ordering. */
class ClaimOrderingTest {

    private JobRepository repo;
    private NamedParameterJdbcTemplate jdbc;

    @BeforeEach
    void setUp() {
        PostgresSupport.reset();
        jdbc = PostgresSupport.jdbc();
        repo = new JobRepository(jdbc);
    }

    @Test
    void higherPriorityIsClaimedFirst() {
        Job low = repo.enqueue(EnqueueRequest.of("t", "q", "x", "{\"n\":\"low\"}").withPriority(0)).job();
        Job high = repo.enqueue(EnqueueRequest.of("t", "q", "x", "{\"n\":\"high\"}").withPriority(10)).job();
        Job mid = repo.enqueue(EnqueueRequest.of("t", "q", "x", "{\"n\":\"mid\"}").withPriority(5)).job();

        assertThat(repo.claim("q", "w", 30, 1)).extracting(Job::id).containsExactly(high.id());
        assertThat(repo.claim("q", "w", 30, 1)).extracting(Job::id).containsExactly(mid.id());
        assertThat(repo.claim("q", "w", 30, 1)).extracting(Job::id).containsExactly(low.id());
        assertThat(repo.claim("q", "w", 30, 1)).isEmpty();
    }

    @Test
    void samePriorityIsClaimedInRunAtOrder() {
        Instant base = Instant.now().minusSeconds(100);
        Job third = repo.enqueue(EnqueueRequest.of("t", "q", "x", null).withRunAt(base.plusSeconds(30))).job();
        Job first = repo.enqueue(EnqueueRequest.of("t", "q", "x", null).withRunAt(base)).job();
        Job second = repo.enqueue(EnqueueRequest.of("t", "q", "x", null).withRunAt(base.plusSeconds(10))).job();

        List<Job> claimed = repo.claim("q", "w", 30, 10);
        assertThat(claimed).extracting(Job::id).containsExactly(first.id(), second.id(), third.id());
    }

    @Test
    void jobWithFutureRunAtIsNotClaimedUntilItIsDue() {
        Job future = repo.enqueue(EnqueueRequest.of("t", "q", "x", null).withRunAt(Instant.now().plusSeconds(3600))).job();
        Job now = repo.enqueue(EnqueueRequest.of("t", "q", "x", null)).job();

        assertThat(repo.claim("q", "w", 30, 10)).extracting(Job::id).containsExactly(now.id());
        assertThat(repo.claim("q", "w", 30, 10)).isEmpty();
        assertThat(repo.findById(future.id()).orElseThrow().state()).isEqualTo(JobState.PENDING);

        // time is PostgreSQL's: pull run_at into the past inside the database and the job becomes claimable
        jdbc.getJdbcTemplate().update("UPDATE jobs SET run_at = now() - interval '1 second' WHERE id = ?", future.id());
        assertThat(repo.claim("q", "w", 30, 10)).extracting(Job::id).containsExactly(future.id());
    }

    @Test
    void queuesAreIsolated() {
        repo.enqueue(EnqueueRequest.of("t", "other", "x", null));
        assertThat(repo.claim("q", "w", 30, 10)).isEmpty();
        assertThat(repo.claim("other", "w", 30, 10)).hasSize(1);
    }

    @Test
    void claimSetsLeaseAttemptAndStartedAt() {
        Job queued = repo.enqueue(EnqueueRequest.of("t", "q", "x", null)).job();
        Job claimed = repo.claim("q", "worker-7", 30, 1).get(0);

        assertThat(claimed.id()).isEqualTo(queued.id());
        assertThat(claimed.state()).isEqualTo(JobState.RUNNING);
        assertThat(claimed.attempt()).isEqualTo(1);
        assertThat(claimed.leaseOwner()).isEqualTo("worker-7");
        assertThat(claimed.startedAt()).isNotNull();
        assertThat(claimed.leaseExpiresAt()).isAfter(claimed.startedAt());
    }
}
