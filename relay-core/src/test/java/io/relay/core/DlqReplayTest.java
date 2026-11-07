package io.relay.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.UUID;

import io.relay.testing.PostgresSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/** Phase 2 test (d): DLQ replay grants extra attempts, never resets attempt, zombies stay fenced out. */
class DlqReplayTest {

    private JobRepository repo;
    private NamedParameterJdbcTemplate jdbc;

    @BeforeEach
    void setUp() {
        PostgresSupport.reset();
        jdbc = PostgresSupport.jdbc();
        repo = new JobRepository(jdbc);
    }

    @Test
    void replayedJobGetsExtraAttemptsAttemptKeepsGrowingAndPreReplayZombieIsRejected() {
        UUID id = repo.enqueue(EnqueueRequest.of("acme", "q", "work", null).withMaxAttempts(2)).job().id();

        // attempt 1 is claimed by a worker that becomes a zombie; its lease is forced to expire
        Job zombie = repo.claim("q", "zombie", 30, 1).get(0);
        jdbc.getJdbcTemplate().update("UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = ?", id);
        assertThat(repo.reapExpired(10)).hasSize(1);

        // attempt 2 fails permanently -> DEAD
        Job second = repo.claim("q", "w2", 30, 1).get(0);
        assertThat(second.attempt()).isEqualTo(2);
        assertThat(repo.fail(id, 2, "still broken", true).outcome()).isEqualTo(FailResult.Outcome.DEAD);
        Job dead = repo.findById(id).orElseThrow();
        assertThat(dead.state()).isEqualTo(JobState.DEAD);
        assertThat(dead.attempt()).isEqualTo(2);
        assertThat(repo.listDead("acme", "q", 10, 0)).extracting(Job::id).containsExactly(id);

        // replay with 3 extra attempts
        assertThat(repo.replay(id, 3)).isTrue();
        Job replayed = repo.findById(id).orElseThrow();
        assertThat(replayed.state()).isEqualTo(JobState.PENDING);
        assertThat(replayed.attempt()).as("attempt must never be reset").isEqualTo(2);
        assertThat(replayed.maxAttempts()).isEqualTo(5);
        assertThat(replayed.finishedAt()).isNull();

        // the new execution gets attempt 3, not a reused token
        Job third = repo.claim("q", "w3", 30, 1).get(0);
        assertThat(third.attempt()).isEqualTo(3);

        // zombies from before the replay (attempt 1 and attempt 2) are all rejected
        assertThat(repo.complete(id, zombie.attempt())).isFalse();
        assertThat(repo.complete(id, second.attempt())).isFalse();
        assertThat(repo.fail(id, second.attempt(), "late", true)).isEqualTo(FailResult.STALE);
        assertThat(repo.heartbeat(id, zombie.attempt(), 30)).isFalse();

        assertThat(repo.complete(id, third.attempt())).isTrue();
        assertThat(repo.findById(id).orElseThrow().state()).isEqualTo(JobState.SUCCEEDED);
    }

    @Test
    void replayedJobCanBeRetriedExactlyExtraAttemptsTimesBeforeDyingAgain() {
        UUID id = repo.enqueue(EnqueueRequest.of("acme", "q", "work", null).withMaxAttempts(1)).job().id();
        Job first = repo.claim("q", "w", 30, 1).get(0);
        assertThat(repo.fail(id, first.attempt(), "e", true).outcome()).isEqualTo(FailResult.Outcome.DEAD);

        assertThat(repo.replay(id, 2)).isTrue();
        for (int expected = 2; expected <= 3; expected++) {
            Job job = repo.claim("q", "w", 30, 1).get(0);
            assertThat(job.attempt()).isEqualTo(expected);
            FailResult r = repo.fail(id, job.attempt(), "e", true);
            if (expected == 3) {
                assertThat(r.outcome()).isEqualTo(FailResult.Outcome.DEAD);
            } else {
                assertThat(r.outcome()).isEqualTo(FailResult.Outcome.RETRIED);
                jdbc.getJdbcTemplate().update("UPDATE jobs SET run_at = now() WHERE id = ?", id);
            }
        }
        assertThat(repo.findById(id).orElseThrow().attempt()).isEqualTo(3);
    }

    @Test
    void onlyDeadJobsCanBeReplayed() {
        UUID id = repo.enqueue(EnqueueRequest.of("acme", "q", "work", null)).job().id();
        assertThat(repo.replay(id, 3)).as("PENDING").isFalse();
        repo.claim("q", "w", 30, 1);
        assertThat(repo.replay(id, 3)).as("RUNNING").isFalse();
        repo.complete(id, 1);
        assertThat(repo.replay(id, 3)).as("SUCCEEDED").isFalse();
        assertThat(repo.replay(UUID.randomUUID(), 3)).as("unknown").isFalse();
        assertThatThrownBy(() -> repo.replay(id, 0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void dlqListingIsFilteredAndPaginated() {
        for (int i = 0; i < 5; i++) {
            killJob("acme", "emails");
        }
        for (int i = 0; i < 2; i++) {
            killJob("acme", "reports");
        }
        killJob("globex", "emails");

        assertThat(repo.countDead(null, null)).isEqualTo(8);
        assertThat(repo.countDead("acme", null)).isEqualTo(7);
        assertThat(repo.countDead("acme", "emails")).isEqualTo(5);
        assertThat(repo.countDead(null, "emails")).isEqualTo(6);

        var page1 = repo.listDead("acme", "emails", 3, 0);
        var page2 = repo.listDead("acme", "emails", 3, 3);
        assertThat(page1).hasSize(3);
        assertThat(page2).hasSize(2);
        assertThat(page1).extracting(Job::id).doesNotContainAnyElementsOf(page2.stream().map(Job::id).toList());

        var stats = repo.queueStats();
        assertThat(stats.get("emails").dead()).isEqualTo(6);
        assertThat(stats.get("reports").dead()).isEqualTo(2);
    }

    private void killJob(String tenant, String queue) {
        UUID id = repo.enqueue(EnqueueRequest.of(tenant, queue, "x", null).withMaxAttempts(1)).job().id();
        Job job = repo.claim(queue, "w", 30, 100).stream().filter(j -> j.id().equals(id)).findFirst().orElseThrow();
        repo.fail(id, job.attempt(), "dead", false);
    }
}
