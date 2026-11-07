package io.relay.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import io.relay.testing.PostgresSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/** Phase 2 test (b): retries, full jitter bounds, DEAD after exactly max_attempts. */
class RetryBackoffTest {

    private static final RetryPolicy POLICY = new RetryPolicy(0.5, 3.0);

    private JobRepository repo;
    private NamedParameterJdbcTemplate jdbc;

    @BeforeEach
    void setUp() {
        PostgresSupport.reset();
        jdbc = PostgresSupport.jdbc();
        repo = new JobRepository(jdbc, POLICY);
    }

    @Test
    void alwaysFailingJobEndsDeadAfterExactlyMaxAttemptsWithDelaysInsideTheJitterBound() {
        int maxAttempts = 6;
        UUID id = repo.enqueue(EnqueueRequest.of("acme", "q", "boom", null).withMaxAttempts(maxAttempts)).job().id();

        List<FailResult> results = new ArrayList<>();
        for (int expectedAttempt = 1; expectedAttempt <= maxAttempts; expectedAttempt++) {
            Job job = repo.claim("q", "w", 30, 1).get(0);
            assertThat(job.attempt()).isEqualTo(expectedAttempt);
            FailResult r = repo.fail(id, job.attempt(), "boom #" + expectedAttempt, true);
            results.add(r);
            if (r.outcome() == FailResult.Outcome.RETRIED) {
                // delay is in [0, min(cap, base * 2^attempt)]
                assertThat(r.delaySecs()).isBetween(0.0, POLICY.maxDelaySecs(expectedAttempt) + 0.001);
                // do not wait for the real delay: make the retry due now
                jdbc.getJdbcTemplate().update("UPDATE jobs SET run_at = now() WHERE id = ?", id);
            }
        }

        assertThat(results.subList(0, maxAttempts - 1)).allMatch(r -> r.outcome() == FailResult.Outcome.RETRIED);
        assertThat(results.get(maxAttempts - 1).outcome()).isEqualTo(FailResult.Outcome.DEAD);

        Job dead = repo.findById(id).orElseThrow();
        assertThat(dead.state()).isEqualTo(JobState.DEAD);
        assertThat(dead.attempt()).isEqualTo(maxAttempts);
        assertThat(dead.lastError()).isEqualTo("boom #" + maxAttempts);
        assertThat(dead.finishedAt()).isNotNull();
        assertThat(repo.claim("q", "w", 30, 1)).isEmpty();
    }

    @Test
    void delaysAreSpreadAcrossTheWholeIntervalAndNeverExceedTheCap() {
        int samples = 300;
        // seed jobs already at attempt 3 (bound = min(3, 0.5 * 8) = 3, i.e. capped) and attempt 1 (bound = 1)
        double maxCapped = 0, minCapped = Double.MAX_VALUE, sumCapped = 0, maxLow = 0;
        for (int i = 0; i < samples; i++) {
            UUID capped = runningJobAtAttempt(3, 10);
            FailResult r = repo.fail(capped, 3, "e", true);
            maxCapped = Math.max(maxCapped, r.delaySecs());
            minCapped = Math.min(minCapped, r.delaySecs());
            sumCapped += r.delaySecs();

            UUID low = runningJobAtAttempt(1, 10);
            maxLow = Math.max(maxLow, repo.fail(low, 1, "e", true).delaySecs());
        }
        assertThat(maxCapped).isLessThanOrEqualTo(3.001);
        assertThat(maxLow).isLessThanOrEqualTo(1.001);
        // full jitter: samples cover the interval (not a constant, not a narrow band)
        assertThat(minCapped).isLessThan(0.6);
        assertThat(maxCapped).isGreaterThan(2.4);
        assertThat(sumCapped / samples).isBetween(1.2, 1.8);
    }

    @Test
    void hugeAttemptNumbersDoNotOverflowTheExponent() {
        UUID id = runningJobAtAttempt(1_500, 2_000);
        FailResult r = repo.fail(id, 1_500, "e", true);
        assertThat(r.outcome()).isEqualTo(FailResult.Outcome.RETRIED);
        assertThat(r.delaySecs()).isBetween(0.0, POLICY.capSecs() + 0.001);
    }

    @Test
    void nonRetryableFailureGoesStraightToDead() {
        UUID id = repo.enqueue(EnqueueRequest.of("acme", "q", "bad-input", null).withMaxAttempts(10)).job().id();
        Job job = repo.claim("q", "w", 30, 1).get(0);
        FailResult r = repo.fail(id, job.attempt(), "invalid payload", false);
        assertThat(r.outcome()).isEqualTo(FailResult.Outcome.DEAD);
        Job dead = repo.findById(id).orElseThrow();
        assertThat(dead.state()).isEqualTo(JobState.DEAD);
        assertThat(dead.attempt()).isEqualTo(1);
        assertThat(dead.leaseOwner()).isNull();
    }

    private UUID runningJobAtAttempt(int attempt, int maxAttempts) {
        UUID id = repo.enqueue(EnqueueRequest.of("acme", "q", "x", null)).job().id();
        // max_attempts is set in SQL: replay can push it beyond the API limit (attempt + extra), which is exactly
        // the case where an unclamped power(2, attempt) would overflow
        jdbc.getJdbcTemplate().update("""
                UPDATE jobs SET state = 'RUNNING', attempt = ?, max_attempts = ?, lease_owner = 'w',
                                lease_expires_at = now() + interval '1 hour' WHERE id = ?
                """, attempt, maxAttempts, id);
        return id;
    }
}
